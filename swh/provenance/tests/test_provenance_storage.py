# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect

from ..interface import ProvenanceInterface, ProvenanceStorageInterface


def test_types(provenance: ProvenanceInterface) -> None:
    """Checks all methods of ProvenanceStorageInterface are implemented by this
    backend, and that they have the same signature."""
    # Create an instance of the protocol (which cannot be instantiated
    # directly, so this creates a subclass, then instantiates it)
    interface = type("_", (ProvenanceStorageInterface,), {})()
    storage = provenance.storage

    assert "content_find_first" in dir(interface)

    missing_methods = []

    for meth_name in dir(interface):
        if meth_name.startswith("_"):
            continue
        interface_meth = getattr(interface, meth_name)
        try:
            concrete_meth = getattr(storage, meth_name)
        except AttributeError:
            if not getattr(interface_meth, "deprecated_endpoint", False):
                # The backend is missing a (non-deprecated) endpoint
                missing_methods.append(meth_name)
            continue

        expected_signature = inspect.signature(interface_meth)
        actual_signature = inspect.signature(concrete_meth)

        assert expected_signature == actual_signature, meth_name

    assert missing_methods == []

    # If all the assertions above succeed, then this one should too.
    # But there's no harm in double-checking.
    # And we could replace the assertions above by this one, but unlike
    # the assertions above, it doesn't explain what is missing.
    assert isinstance(storage, ProvenanceStorageInterface)
