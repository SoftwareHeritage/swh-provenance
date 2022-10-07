# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import asdict
from typing import Dict, Generator

import pytest

from swh.provenance import get_provenance_storage
from swh.provenance.storage.interface import (
    EntityType,
    ProvenanceStorageInterface,
    RelationType,
)

from .test_provenance_storage import TestProvenanceStorage as _TestProvenanceStorage


@pytest.fixture()
def provenance_storage(
    provenance_postgresqldb: Dict[str, str],
) -> Generator[ProvenanceStorageInterface, None, None]:
    cfg = {
        "storage": {
            "cls": "postgresql",
            "db": provenance_postgresqldb,
            "raise_on_commit": True,
        },
        "journal_writer": {
            "cls": "memory",
        },
    }
    with get_provenance_storage(cls="journal", **cfg) as storage:
        yield storage


class TestProvenanceStorageJournal(_TestProvenanceStorage):
    def test_provenance_storage_content(self, provenance_storage):
        super().test_provenance_storage_content(provenance_storage)
        assert provenance_storage.journal
        objtypes = {objtype for (objtype, obj) in provenance_storage.journal.objects}
        assert objtypes == {"content"}

        journal_objs = {
            obj.id
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "content"
        }
        assert provenance_storage.entity_get_all(EntityType.CONTENT) == journal_objs

    def test_provenance_storage_directory(self, provenance_storage):
        super().test_provenance_storage_directory(provenance_storage)
        assert provenance_storage.journal
        objtypes = {objtype for (objtype, obj) in provenance_storage.journal.objects}
        assert objtypes == {"directory"}

        journal_objs = {
            obj.id
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "directory"
        }
        assert provenance_storage.entity_get_all(EntityType.DIRECTORY) == journal_objs

    def test_provenance_storage_location(self, provenance_storage):
        super().test_provenance_storage_location(provenance_storage)
        assert provenance_storage.journal
        objtypes = {objtype for (objtype, obj) in provenance_storage.journal.objects}
        assert objtypes == {"location"}

        journal_objs = {
            obj.id: obj.value
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "location"
        }
        assert provenance_storage.location_get_all() == journal_objs

    def test_provenance_storage_orign(self, provenance_storage):
        super().test_provenance_storage_origin(provenance_storage)
        assert provenance_storage.journal
        objtypes = {objtype for (objtype, obj) in provenance_storage.journal.objects}
        assert objtypes == {"origin"}

        journal_objs = {
            obj.id
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "origin"
        }
        assert provenance_storage.entity_get_all(EntityType.ORIGIN) == journal_objs

    def test_provenance_storage_revision(self, provenance_storage):
        super().test_provenance_storage_revision(provenance_storage)
        assert provenance_storage.journal
        objtypes = {objtype for (objtype, obj) in provenance_storage.journal.objects}
        assert objtypes == {"revision", "origin"}

        journal_objs = {
            obj.id
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "revision"
        }
        assert provenance_storage.entity_get_all(EntityType.REVISION) == journal_objs

    def test_provenance_storage_relation_revision_layer(self, provenance_storage):
        super().test_provenance_storage_relation_revision_layer(provenance_storage)
        assert provenance_storage.journal
        objtypes = {objtype for (objtype, obj) in provenance_storage.journal.objects}
        assert objtypes == {
            "location",
            "content",
            "directory",
            "revision",
            "content_in_revision",
            "content_in_directory",
            "directory_in_revision",
        }

        journal_rels = {
            obj.id: {tuple(v.items()) for v in obj.value}
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "content_in_revision"
        }
        prov_rels = {
            k: {tuple(asdict(reldata).items()) for reldata in v}
            for k, v in provenance_storage.relation_get_all(
                RelationType.CNT_EARLY_IN_REV
            ).items()
        }
        assert prov_rels == journal_rels

        journal_rels = {
            obj.id: {tuple(v.items()) for v in obj.value}
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "content_in_directory"
        }
        prov_rels = {
            k: {tuple(asdict(reldata).items()) for reldata in v}
            for k, v in provenance_storage.relation_get_all(
                RelationType.CNT_IN_DIR
            ).items()
        }
        assert prov_rels == journal_rels

        journal_rels = {
            obj.id: {tuple(v.items()) for v in obj.value}
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "directory_in_revision"
        }
        prov_rels = {
            k: {tuple(asdict(reldata).items()) for reldata in v}
            for k, v in provenance_storage.relation_get_all(
                RelationType.DIR_IN_REV
            ).items()
        }
        assert prov_rels == journal_rels

    def test_provenance_storage_relation_origin_layer(self, provenance_storage):
        super().test_provenance_storage_relation_orign_layer(provenance_storage)
        assert provenance_storage.journal
        objtypes = {objtype for (objtype, obj) in provenance_storage.journal.objects}
        assert objtypes == {
            "origin",
            "revision",
            "revision_in_origin",
            "revision_before_revision",
        }

        journal_rels = {
            obj.id: {tuple(v.items()) for v in obj.value}
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "revision_in_origin"
        }
        prov_rels = {
            k: {tuple(asdict(reldata).items()) for reldata in v}
            for k, v in provenance_storage.relation_get_all(
                RelationType.REV_IN_ORG
            ).items()
        }
        assert prov_rels == journal_rels

        journal_rels = {
            obj.id: {tuple(v.items()) for v in obj.value}
            for (objtype, obj) in provenance_storage.journal.objects
            if objtype == "revision_before_revision"
        }
        prov_rels = {
            k: {tuple(asdict(reldata).items()) for reldata in v}
            for k, v in provenance_storage.relation_get_all(
                RelationType.REV_BEFORE_REV
            ).items()
        }
        assert prov_rels == journal_rels
