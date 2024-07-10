# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.model.swhids import CoreSWHID, QualifiedSWHID


from swh.graph import example_dataset as data


class TestProvenance:
    def test_whereis_content_with_rel(self, swh_provenance):
        """run whereis on a Content associated with a release and an origin

        The `whereis` logic should use the release as the anchor use the origin
        url for the QualifiedSWHID
        """
        source = data.CONTENTS[0].swhid()
        result = swh_provenance.whereis(source)
        target = QualifiedSWHID(
            object_type=source.object_type,
            object_id=source.object_id,
            anchor=data.RELEASES[0].swhid(),
            origin="https://example.com/swh/graph2",
        )
        assert result == target, result

    def test_whereis_directory_with_rel(self, swh_provenance):
        """run whereis on a Directory associated with a release and an origin

        The `whereis` logic should use the release as the anchor use the origin
        url for the QualifiedSWHID
        """
        source = data.DIRECTORIES[1].swhid()
        result = swh_provenance.whereis(source)
        target = QualifiedSWHID(
            object_type=source.object_type,
            object_id=source.object_id,
            anchor=data.RELEASES[0].swhid(),
            origin="https://example.com/swh/graph2",
        )
        assert result == target, result

    def test_whereis_content_with_rev(self, swh_provenance):
        """run whereis on a Directory associated with a revision and an origin

        Since there is not associated release, the `whereis` logic should use
        the revision as the anchor use the origin url for the QualifiedSWHID
        """
        source = data.CONTENTS[4].swhid()
        result = swh_provenance.whereis(source)
        target = QualifiedSWHID(
            object_type=source.object_type,
            object_id=source.object_id,
            anchor=data.REVISIONS[2].swhid(),
            origin="https://example.com/swh/graph2",
        )
        assert result == target, result

    def test_whereis_directory_with_rev(self, swh_provenance):
        """run whereis on a Directory associated with a revision and an origin

        Since there is not associated release, the `whereis` logic should use
        the revision as the anchor use the origin url for the QualifiedSWHID
        """
        source = data.DIRECTORIES[3].swhid()
        result = swh_provenance.whereis(source)
        target = QualifiedSWHID(
            object_type=source.object_type,
            object_id=source.object_id,
            anchor=data.REVISIONS[2].swhid(),
            origin="https://example.com/swh/graph2",
        )
        assert result == target, result

    def test_whereis_content_no_anchor(self, swh_provenance):
        """run whereis on a Content associated with no anchor"""

        if len(data.CONTENTS) < 7:
            # waiting on swh-graph data set upgrade within !547
            pytest.skip("no dangling Content in the test dataset")
        for source in (
            data.CONTENTS[6].swhid(),
            data.CONTENTS[7].swhid(),
            data.CONTENTS[8].swhid(),
        ):
            result = swh_provenance.whereis(source)
            assert result is None

    def test_whereis_directory_no_anchor(self, swh_provenance):
        """run whereis on a Directory associated with no anchor"""

        if len(data.DIRECTORIES) < 7:
            # waiting on swh-graph data set upgrade within !547
            pytest.skip("no dangling Directoryin the test dataset")
        source = data.DIRECTORIES[6].swhid()
        result = swh_provenance.whereis(source)
        assert result is None

    def test_whereis_content_unknown(self, swh_provenance):
        """The requested object is unknown, we will return None"""
        source = CoreSWHID.from_string(
            "swh:1:cnt:7e5dda5a1a86a6f6ca4275658284f8feda827f90"
        )
        result = swh_provenance.whereis(source)
        assert result is None
