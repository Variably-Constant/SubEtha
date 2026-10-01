//! A process inside an AppContainer attaches to a ring and a notifier set
//! that a process outside it created, through the container's named-object
//! directory. The outside names the container with
//! `ShmNamespace::AppContainer`; the inside uses `ShmNamespace::Session`,
//! whose `Local\` is that directory from within.
//!
//! The inside process is this test binary started again in the container,
//! suspended until the outside has made the ring, since the directory
//! exists only while a process of the container is running. After the
//! inside has attached, the outside grows a producer slot and makes the
//! payload region, so the objects a channel makes later reach the
//! container too.

#![cfg(windows)]

use std::ffi::c_void;
use std::mem::{size_of, zeroed};
use std::os::windows::ffi::OsStrExt;
use std::ptr::{null, null_mut};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::{AdaptiveRing, RingShape};
use subetha_cxc::cross_process_notifier::NotifierSet;
use subetha_cxc::frame_ring::{FrameClass, LayoutHint};
use subetha_cxc::shared_ring::RingError;
use subetha_cxc::shm_file::{ContainerSid, ShmNamespace};
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0};
use windows_sys::Win32::Security::Isolation::{
    CreateAppContainerProfile, DeleteAppContainerProfile, DeriveAppContainerSidFromAppContainerName,
};
use windows_sys::Win32::Security::{FreeSid, PSID, SECURITY_CAPABILITIES};
use windows_sys::Win32::System::Threading::{
    CreateProcessW, DeleteProcThreadAttributeList, GetExitCodeProcess,
    InitializeProcThreadAttributeList, ResumeThread, TerminateProcess, UpdateProcThreadAttribute,
    WaitForSingleObject, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, EXTENDED_STARTUPINFO_PRESENT,
    LPPROC_THREAD_ATTRIBUTE_LIST, PROCESS_INFORMATION, PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
    STARTUPINFOEXW,
};

/// The name of the container's profile, which the test makes and deletes.
const MONIKER: &str = "subetha.cxc.appcontainer.test";

/// Tells the binary started in the container which ring to join.
const RING_ENV: &str = "SUBETHA_APPCONTAINER_RING";

/// How long one side waits for the other before the exchange counts as
/// lost, the bound the waker's cross-process tests put on theirs.
const LOST: Duration = Duration::from_secs(30);

/// The ring's capacity, the one the crate's other ring tests use.
const CAPACITY: usize = 64;

/// What the process in the container found, by its exit code.
fn in_container_step(code: u32) -> &'static str {
    match code {
        0 => "received both frames",
        1 => "could not attach to the ring",
        2 => "could not attach a notifier",
        3 => "was not signaled through its notifier",
        4 => "could not receive what was sent",
        5 => "received something other than what was sent",
        _ => "exited without saying why",
    }
}

/// The role this binary plays when started in the container: a no-op
/// unless the parent named a ring.
#[test]
fn in_container() {
    let Some(name) = std::env::var_os(RING_ENV) else {
        return;
    };
    let name = name.to_string_lossy().into_owned();
    std::process::exit(join_from_inside(&name));
}

/// Attach to the ring and a notifier from inside the container, wait for
/// the signal, and take the two frames: one from the producer slot grown
/// after the attach, one carried by the payload region made after it.
fn join_from_inside(name: &str) -> i32 {
    let ring = match AdaptiveRing::open_shmfs_in(name, 1, 1, CAPACITY, ShmNamespace::Session) {
        Ok(ring) => ring,
        Err(e) => {
            eprintln!("in the container: attaching to {name}: {e:?}");
            return 1;
        }
    };
    if let Err(e) = ring.morph_to(RingShape::Mpsc) {
        eprintln!("in the container: taking the per-producer shape: {e:?}");
        return 1;
    }
    let notifier = match NotifierSet::shm(name, ShmNamespace::Session, None).and_then(|set| set.attach()) {
        Ok(notifier) => notifier,
        Err(e) => {
            eprintln!("in the container: attaching a notifier: {e:?}");
            return 2;
        }
    };
    if !notifier.wait(LOST.as_millis() as i32) {
        eprintln!("in the container: no signal within {LOST:?}");
        return 3;
    }
    let deadline = Instant::now() + LOST;
    let mut got = Vec::new();
    while got.len() < 2 {
        let mut out = Vec::new();
        match ring.recv_frame(0, &mut out) {
            Ok(class) => got.push((class, out)),
            Err(RingError::Empty) if Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => {
                eprintln!("in the container: receiving after {} frames: {e:?}", got.len());
                return 4;
            }
        }
    }
    let grown = got.iter().any(|(class, out)| *class == FrameClass::Inline && out == b"grown");
    let framed = got.iter().any(|(class, out)| *class == FrameClass::Offset && out == b"framed");
    if !(grown && framed) {
        eprintln!("in the container: received {got:?}");
        return 5;
    }
    0
}

/// An AppContainer profile the test made, deleted when the test ends.
struct Profile {
    sid: PSID,
    name: Vec<u16>,
}

impl Profile {
    fn create(moniker: &str) -> Self {
        let name: Vec<u16> = moniker.encode_utf16().chain(Some(0)).collect();
        let mut sid = null_mut();
        // SAFETY: name is NUL-terminated; sid receives a SID freed on drop.
        let made = unsafe {
            CreateAppContainerProfile(name.as_ptr(), name.as_ptr(), name.as_ptr(), null(), 0, &mut sid)
        };
        if made < 0 {
            // A profile an earlier run left behind has the same SID.
            // SAFETY: as above.
            let derived = unsafe { DeriveAppContainerSidFromAppContainerName(name.as_ptr(), &mut sid) };
            assert!(
                derived >= 0,
                "no AppContainer profile {moniker} could be made ({made:#x}) or found ({derived:#x})"
            );
        }
        Self { sid, name }
    }
}

impl Drop for Profile {
    fn drop(&mut self) {
        // SAFETY: the SID came from the profile call and is freed once.
        unsafe { FreeSid(self.sid) };
        // SAFETY: name is NUL-terminated.
        let deleted = unsafe { DeleteAppContainerProfile(self.name.as_ptr()) };
        if deleted < 0 {
            eprintln!("the test's AppContainer profile was not deleted: {deleted:#x}");
        }
    }
}

/// This binary, started in the container and ended with the test.
struct InContainer {
    process: HANDLE,
    thread: HANDLE,
}

impl InContainer {
    /// Start this binary's `in_container` role in `profile`'s container,
    /// suspended, told the ring to join through its environment.
    fn start_suspended(profile: &Profile, ring: &str) -> Self {
        let exe = std::env::current_exe().expect("the test binary's own path");
        let mut command: Vec<u16> =
            format!("\"{}\" in_container --exact --nocapture", exe.display())
                .encode_utf16()
                .chain(Some(0))
                .collect();
        let mut environment = environment_with(RING_ENV, ring);

        let mut capabilities = SECURITY_CAPABILITIES {
            AppContainerSid: profile.sid,
            Capabilities: null_mut(),
            CapabilityCount: 0,
            Reserved: 0,
        };
        let mut size = 0usize;
        // SAFETY: a null list with a size pointer asks only for the size.
        unsafe { InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut size) };
        let mut list_buffer = vec![0u64; size.div_ceil(size_of::<u64>())];
        let list = list_buffer.as_mut_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST;
        // SAFETY: list addresses `size` writable bytes; capabilities
        // outlives the CreateProcessW call below.
        let listed = unsafe {
            InitializeProcThreadAttributeList(list, 1, 0, &mut size) != 0
                && UpdateProcThreadAttribute(
                    list,
                    0,
                    PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES as usize,
                    &mut capabilities as *mut SECURITY_CAPABILITIES as *const c_void,
                    size_of::<SECURITY_CAPABILITIES>(),
                    null_mut(),
                    null(),
                ) != 0
        };
        assert!(listed, "the container's attribute list: {}", std::io::Error::last_os_error());

        // SAFETY: STARTUPINFOEXW is plain data, valid zeroed.
        let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
        startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        startup.lpAttributeList = list;
        // SAFETY: as above.
        let mut info: PROCESS_INFORMATION = unsafe { zeroed() };
        // SAFETY: command and environment are NUL-terminated and writable;
        // startup names the attribute list built above.
        let started = unsafe {
            CreateProcessW(
                null(),
                command.as_mut_ptr(),
                null(),
                null(),
                0,
                EXTENDED_STARTUPINFO_PRESENT | CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT,
                environment.as_mut_ptr() as *const c_void,
                null(),
                &startup.StartupInfo,
                &mut info,
            )
        };
        let start_error = std::io::Error::last_os_error();
        // SAFETY: the list was initialized above and is not used again.
        unsafe { DeleteProcThreadAttributeList(list) };
        assert!(started != 0, "starting this binary in the container: {start_error}");
        Self { process: info.hProcess, thread: info.hThread }
    }

    fn resume(&self) {
        // SAFETY: the thread handle is this value's and open.
        let previous = unsafe { ResumeThread(self.thread) };
        assert!(previous != u32::MAX, "resuming the container's process: {}", std::io::Error::last_os_error());
    }

    /// Its exit code once it has exited within `bound`, else `None`.
    fn exit_code_within(&self, bound: Duration) -> Option<u32> {
        // SAFETY: the process handle is this value's and open.
        if unsafe { WaitForSingleObject(self.process, bound.as_millis() as u32) } != WAIT_OBJECT_0 {
            return None;
        }
        let mut code = 0u32;
        // SAFETY: as above; code is writable.
        let read = unsafe { GetExitCodeProcess(self.process, &mut code) };
        assert!(read != 0, "reading the container process's exit code: {}", std::io::Error::last_os_error());
        Some(code)
    }
}

impl Drop for InContainer {
    fn drop(&mut self) {
        // A process the test started and is not waiting for any more ends
        // with it, rather than running on in the container.
        if self.exit_code_within(Duration::ZERO).is_none() {
            // SAFETY: the process handle is this value's and open.
            unsafe { TerminateProcess(self.process, 1) };
        }
        // SAFETY: both handles are this value's and closed once.
        unsafe {
            CloseHandle(self.thread);
            CloseHandle(self.process);
        }
    }
}

/// This process's environment with `key` set to `value`, as the
/// NUL-separated, double-NUL-terminated block CreateProcessW takes.
fn environment_with(key: &str, value: &str) -> Vec<u16> {
    let mut block = Vec::new();
    for (k, v) in std::env::vars_os() {
        if k == key {
            continue;
        }
        block.extend(k.encode_wide());
        block.push(u16::from(b'='));
        block.extend(v.encode_wide());
        block.push(0);
    }
    block.extend(key.encode_utf16());
    block.push(u16::from(b'='));
    block.extend(value.encode_utf16());
    block.push(0);
    block.push(0);
    block
}

#[test]
fn a_process_in_an_appcontainer_attaches_what_an_outside_process_created() {
    let profile = Profile::create(MONIKER);
    let sid = ContainerSid::from_container_name(MONIKER).expect("the container's SID");
    let namespace = ShmNamespace::AppContainer(sid);
    // Administrators, authenticated users and the container itself, with a
    // low mandatory label so the container's low integrity may write.
    let sddl = format!("D:P(A;;GA;;;BA)(A;;GA;;;AU)(A;;GA;;;{sid})S:(ML;;NW;;;LW)");
    let name = format!("subetha_ac_{}", std::process::id());

    let inside = InContainer::start_suspended(&profile, &name);
    let ring = AdaptiveRing::create_shmfs_secured(&name, 1, 1, CAPACITY, namespace, Some(&sddl))
        .expect("the ring is made in the container's directory");
    ring.morph_to(RingShape::Mpsc).expect("the per-producer shape");
    let notifiers = NotifierSet::shm(&name, namespace, Some(&sddl))
        .expect("the notifier record is made in the container's directory");
    inside.resume();

    let began = Instant::now();
    while notifiers.attached() == 0 {
        if let Some(code) = inside.exit_code_within(Duration::ZERO) {
            panic!("the process in the container exited ({code}): it {}", in_container_step(code));
        }
        assert!(began.elapsed() < LOST, "the process in the container attached no notifier within {LOST:?}");
        std::thread::yield_now();
    }

    let first = ring.register_producer().expect("a producer");
    let grown = ring.register_producer().expect("a producer past the hint grows the ring in the container");
    ring.send_frame_as(grown, b"grown", LayoutHint::ForceInline)
        .expect("a frame into the backing grown after the attach");
    ring.send_frame_as(first, b"framed", LayoutHint::ForceOffset)
        .expect("a frame through the payload region made after the attach");
    assert!(
        notifiers.signal() >= 1,
        "the notifier the container attached is signaled from outside it"
    );

    let code = inside
        .exit_code_within(LOST)
        .unwrap_or_else(|| panic!("the process in the container did not finish within {LOST:?}"));
    assert_eq!(code, 0, "the process in the container {}", in_container_step(code));
}
