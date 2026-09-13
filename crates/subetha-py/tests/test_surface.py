"""The package's public surface, checked against itself.

A binding is only a real Python surface if editors and type checkers can
see it, which means the stub beside the extension has to stay in step
with the extension. Nothing keeps it there on its own: a class added to
the Rust and to __init__.py but not to the stub is invisible to every
type checker and nobody finds out until a user reports it.

These tests are what keeps them in step.
"""

import ast
import os

import subetha


def stub_source():
    stub = os.path.join(os.path.dirname(subetha.__file__), "__init__.pyi")
    assert os.path.exists(stub), f"the package ships no stub at {stub}"
    with open(stub, encoding="utf-8") as handle:
        return handle.read()


def stub_names():
    tree = ast.parse(stub_source())
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
    return names


def test_the_package_ships_a_py_typed_marker():
    marker = os.path.join(os.path.dirname(subetha.__file__), "py.typed")
    assert os.path.exists(marker), "without py.typed a type checker ignores the stub"


def test_every_exported_name_appears_in_the_stub():
    missing = sorted(name for name in subetha.__all__ if name not in stub_names())
    assert not missing, f"exported but absent from the stub: {missing}"


def names_of_transports_not_built():
    """Stub names belonging to a transport this wheel was built without.

    The stub covers every transport, because a type checker should see
    the same surface whichever wheel is installed. A wheel built without
    one exports none of its classes, and those are the names to let
    through here rather than a blanket exemption.
    """
    absent = set()
    for transport, classes in subetha.OPTIONAL_BY_TRANSPORT.items():
        if transport not in subetha.transports:
            absent.update(classes)
    return absent


def test_the_stub_promises_nothing_the_package_lacks():
    allowed = names_of_transports_not_built()
    extra = sorted(
        name
        for name in stub_names()
        if not name.startswith("_")
        and name not in subetha.__all__
        and name not in allowed
    )
    assert not extra, f"in the stub but not exported: {extra}"


def test_every_transport_the_wheel_names_brings_its_classes():
    for transport, classes in subetha.OPTIONAL_BY_TRANSPORT.items():
        if transport in subetha.transports:
            for name in classes:
                assert hasattr(subetha, name), (
                    f"{transport} is built but {name} is missing"
                )


def test_the_wheel_always_carries_the_sens_link():
    assert "sens" in subetha.transports
    assert hasattr(subetha, "SensSender")
    assert hasattr(subetha, "SensReceiver")


def test_everything_in_all_is_actually_importable():
    for name in subetha.__all__:
        assert hasattr(subetha, name), f"{name} is in __all__ but not on the module"


def test_the_extension_is_inside_the_package():
    # The package wraps the extension rather than being it, which is what
    # lets the stub and the marker ship beside it.
    assert subetha.__file__.endswith("__init__.py")
    from subetha import _subetha

    assert _subetha is not None
