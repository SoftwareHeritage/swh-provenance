# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List, Optional, Tuple

import pytest

from swh.graph import example_dataset as data
from swh.model.swhids import CoreSWHID, QualifiedSWHID

# a set of (swhid → expectation)
TEST_CASES: Dict[
    str,
    List[Tuple[CoreSWHID, Optional[QualifiedSWHID]]],
] = {
    "content-with-rel": [
        (
            data.CONTENTS[0].swhid(),
            QualifiedSWHID(
                object_type=data.CONTENTS[0].swhid().object_type,
                object_id=data.CONTENTS[0].swhid().object_id,
                anchor=data.RELEASES[0].swhid(),
                origin="https://example.com/swh/graph",
            ),
        )
    ],
    "directory-with-rel": [
        (
            data.DIRECTORIES[1].swhid(),
            QualifiedSWHID(
                object_type=data.DIRECTORIES[1].swhid().object_type,
                object_id=data.DIRECTORIES[1].swhid().object_id,
                anchor=data.RELEASES[0].swhid(),
                origin="https://example.com/swh/graph",
            ),
        )
    ],
    "content-with-rev": [
        (
            data.CONTENTS[4].swhid(),
            QualifiedSWHID(
                object_type=data.CONTENTS[4].swhid().object_type,
                object_id=data.CONTENTS[4].swhid().object_id,
                anchor=data.REVISIONS[2].swhid(),
                origin="https://example.com/swh/graph2",
            ),
        )
    ],
    "directory-with-rev": [
        (
            data.DIRECTORIES[3].swhid(),
            QualifiedSWHID(
                object_type=data.DIRECTORIES[3].swhid().object_type,
                object_id=data.DIRECTORIES[3].swhid().object_id,
                anchor=data.REVISIONS[2].swhid(),
                origin="https://example.com/swh/graph2",
            ),
        )
    ],
    "content-unknown": [
        (
            CoreSWHID.from_string("swh:1:cnt:7e5dda5a1a86a6f6ca4275658284f8feda827f90"),
            None,
        )
    ],
}

if len(data.CONTENTS) >= 7:
    TEST_CASES["content_no_anchor"] = [
        (data.CONTENTS[6].swhid(), None),
        (data.CONTENTS[7].swhid(), None),
        (data.CONTENTS[8].swhid(), None),
    ]

if len(data.DIRECTORIES) >= 7:
    TEST_CASES["directory_no_anchor"] = [(data.DIRECTORIES[6].swhid(), None)]


class TestProvenance:
    def _test_whereis_case(self, swh_provenance, case_id: str):
        for source, target in TEST_CASES[case_id]:
            result = swh_provenance.whereis(swhid=source)
            assert result == target, result

    def test_whereis_content_with_rel(self, swh_provenance):
        """run whereis on a Content associated with a release and an origin

        The `whereis` logic should use the release as the anchor use the origin
        url for the QualifiedSWHID
        """
        self._test_whereis_case(swh_provenance, "content-with-rel")

    def test_whereis_directory_with_rel(self, swh_provenance):
        """run whereis on a Directory associated with a release and an origin

        The `whereis` logic should use the release as the anchor use the origin
        url for the QualifiedSWHID
        """
        self._test_whereis_case(swh_provenance, "directory-with-rel")

    def test_whereis_content_with_rev(self, swh_provenance):
        """run whereis on a Directory associated with a revision and an origin

        Since there is not associated release, the `whereis` logic should use
        the revision as the anchor use the origin url for the QualifiedSWHID
        """
        self._test_whereis_case(swh_provenance, "content-with-rev")

    def test_whereis_directory_with_rev(self, swh_provenance):
        """run whereis on a Directory associated with a revision and an origin

        Since there is not associated release, the `whereis` logic should use
        the revision as the anchor use the origin url for the QualifiedSWHID
        """
        self._test_whereis_case(swh_provenance, "directory-with-rev")

    def test_whereis_content_no_anchor(self, swh_provenance):
        """run whereis on a Content associated with no anchor"""

        if "content-no-anchor" not in TEST_CASES:
            # waiting on swh-graph data set upgrade within !547
            pytest.skip("no dangling Content in the test dataset")
        self._test_whereis_case(swh_provenance, "content-no-anchor")

    def test_whereis_directory_no_anchor(self, swh_provenance):
        """run whereis on a Directory associated with no anchor"""

        if len(data.DIRECTORIES) < 7:
            # waiting on swh-graph data set upgrade within !547
            pytest.skip("no dangling Directory in the test dataset")
        self._test_whereis_case(swh_provenance, "directory-no-anchor")

    def test_whereis_content_unknown(self, swh_provenance):
        """The requested object is unknown, we will return None"""
        self._test_whereis_case(swh_provenance, "content-unknown")

    def test_whereare(self, swh_provenance):
        sources = []
        targets = []
        for cases in TEST_CASES.values():
            for case_source, case_target in cases:
                sources.append(case_source)
                targets.append(case_target)
        result = swh_provenance.whereare(swhids=sources)
        assert result == targets, result
