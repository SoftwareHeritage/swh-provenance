# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING
import warnings

from .interface import ProvenanceStorageInterface


def get_provenance_storage(cls: str, **kwargs) -> ProvenanceStorageInterface:
    """Get an archive object of class ``cls`` with arguments ``args``.

    Args:
        cls: storage's class, only 'local' is currently supported
        args: dictionary of arguments passed to the storage class constructor

    Returns:
        an instance of storage object

    Raises:
        :cls:`ValueError` if passed an unknown archive class.
    """
    if cls in ["local", "postgresql"]:
        from swh.provenance.storage.postgresql import ProvenanceStoragePostgreSql

        if cls == "local":
            warnings.warn(
                '"local" class is deprecated for provenance storage, please '
                'use "postgresql" class instead.',
                DeprecationWarning,
            )

        raise_on_commit = kwargs.get("raise_on_commit", False)
        return ProvenanceStoragePostgreSql(
            raise_on_commit=raise_on_commit, db=kwargs["db"]
        )

    elif cls == "rabbitmq":
        from swh.provenance.storage.rabbitmq.client import (
            ProvenanceStorageRabbitMQClient,
        )

        rmq_storage = ProvenanceStorageRabbitMQClient(**kwargs)
        if TYPE_CHECKING:
            assert isinstance(rmq_storage, ProvenanceStorageInterface)
        return rmq_storage
    elif cls == "journal":
        from swh.journal.writer import get_journal_writer
        from swh.provenance.storage.journal import ProvenanceStorageJournal

        storage = get_provenance_storage(**kwargs["storage"])
        journal = get_journal_writer(**kwargs["journal_writer"])

        ret = ProvenanceStorageJournal(storage=storage, journal=journal)
        return ret

    raise ValueError
