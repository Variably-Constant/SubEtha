//! What a process already holding many gigabytes of mappings gets when
//! it asks for one more.
//!
//! A store that keeps its payloads in large mapped segments and its
//! index in small ones opens both at startup, the payload segments
//! first and held while the index opens. `SharedHashMap::create` on an
//! existing 64 MiB index file can answer `IoError(StorageFull)` there
//! while the same call succeeds on an idle process, and the file is
//! intact either way.
//!
//! Free space is not the cause, and neither is a short file: this varies
//! the one thing left, which is how much a single process has mapped
//! when it makes the call, and whether creating a region and
//! re-attaching one behave the same at that point.
//!
//! Index segments alone do not reproduce it. Twenty create and attach
//! with 1.25 GiB mapped and no error, so the payload segments are what
//! matters and the question is what the small mapping gets after the
//! large ones are in hand.
//!
//! Usage:
//!   index_open_storagefull [segments] [arenas] [dir]
//!
//! `segments` defaults to 20 and `arenas` to 0, the index-only shape.
//! Each arena is 2 GiB of real file, so a run with several writes tens
//! of gigabytes and needs the room. `dir` defaults to a fresh temp
//! directory. It prints one line per region at every stage, so a run
//! that stops early says where and one that survives says how far it
//! got.

use std::path::PathBuf;

use subetha_cxc::shared_hash_map::{map_file_size, SharedHashMap};
use subetha_cxc::shared_string_arena::SharedStringArena;

const SLOTS: usize = 1 << 20;

/// One payload segment, at the size a store of this shape uses.
const SEGMENT_BYTES: usize = 2 * 1024 * 1024 * 1024;

fn main() {
    let mut args = std::env::args().skip(1);
    // A mistyped count must not quietly become the default: this run's
    // whole purpose is the number of regions, so a silent 20 would
    // answer a question nobody asked.
    let segments: usize = match args.next() {
        None => 20,
        Some(a) => match a.parse() {
            Ok(n) => n,
            Err(e) => {
                eprintln!("the segment count {a:?} is not a number ({e})");
                std::process::exit(2);
            }
        },
    };
    let arenas: usize = match args.next() {
        None => 0,
        Some(a) => match a.parse() {
            Ok(n) => n,
            Err(e) => {
                eprintln!("the arena count {a:?} is not a number ({e})");
                std::process::exit(2);
            }
        },
    };
    let dir: PathBuf = match args.next() {
        Some(d) => PathBuf::from(d),
        None => {
            let mut p = std::env::temp_dir();
            p.push(format!("cxc_index_repro_{}", std::process::id()));
            p
        }
    };
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("cannot create {}: {e}", dir.display());
        std::process::exit(2);
    }

    let each = map_file_size(SLOTS);
    println!(
        "dir {}\nsegments {segments}, {each} bytes each, {:.2} GiB total",
        dir.display(),
        (segments * each) as f64 / (1u64 << 30) as f64
    );

    // Create them, as the store did over its life. Each is a fresh file,
    // so this is set_len plus a full zeroing of the mapping.
    let mut made = 0usize;
    for n in 0..segments {
        let p = dir.join(format!("{n:03}.index"));
        match SharedHashMap::<u64, u64>::create(&p, SLOTS) {
            Ok(_m) => {
                made += 1;
                println!("create {n:03}  ok");
            }
            Err(e) => {
                println!("create {n:03}  FAILED {e:?}");
                break;
            }
        }
    }
    println!("created {made} of {segments}");

    // The payload segments, mapped and held before the index is opened,
    // which is the order open_sized uses: every existing segment is
    // attached in its while loop, and only then are the index segments
    // opened. They stay mapped for the life of the store, so the index
    // open happens with all of them held.
    let mut arena_held: Vec<SharedStringArena> = Vec::new();
    for n in 0..arenas {
        let p = dir.join(format!("{n:03}.arena"));
        match SharedStringArena::create(&p, SEGMENT_BYTES) {
            Ok(a) => {
                arena_held.push(a);
                println!(
                    "arena  {n:03}  ok  ({} held, {:.2} GiB)",
                    arena_held.len(),
                    (arena_held.len() * SEGMENT_BYTES) as f64 / (1u64 << 30) as f64
                );
            }
            Err(e) => {
                println!("arena  {n:03}  FAILED {e:?}  ({} held before it)", arena_held.len());
                break;
            }
        }
    }

    // Open every index segment again in a single process and HOLD them,
    // which is what the arena open does - it keeps every index segment
    // mapped for the life of the store. Dropping each before the next
    // would test something the server never does.
    let mut held: Vec<SharedHashMap<u64, u64>> = Vec::new();
    for n in 0..made {
        let p = dir.join(format!("{n:03}.index"));
        match SharedHashMap::<u64, u64>::create(&p, SLOTS) {
            Ok(m) => {
                held.push(m);
                println!("attach {n:03}  ok  ({} held)", held.len());
            }
            Err(e) => {
                println!("attach {n:03}  FAILED {e:?}  ({} held before it)", held.len());
                break;
            }
        }
    }
    let mapped = held.len() * each + arena_held.len() * SEGMENT_BYTES;
    println!(
        "attached {} of {made} index, {} of {arenas} arena; {:.2} GiB mapped in this process",
        held.len(),
        arena_held.len(),
        mapped as f64 / (1u64 << 30) as f64
    );
    let whole = held.len() == made && made == segments && arena_held.len() == arenas;
    println!(
        "VERDICT {}",
        if whole {
            "no failure reproduced at this shape"
        } else {
            "reproduced - see the first FAILED line above"
        }
    );
}
