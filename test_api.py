# pylint: skip-file
from unittest.mock import patch
from datetime import date, datetime

import pytest
from psycopg2 import connect


class TestPerformerRoute_Task_1:
    """Tests for the Performers routes"""

    def test_returns_200_status_code(self, test_api):
        res = test_api.get("/performers")
        assert res.status_code == 200

    def test_rejects_wrong_method_calls(self, test_api):
        """Tests that the api does not accept non GET methods"""
        assert test_api.post("/performers").status_code == 405
        assert test_api.patch("/performers").status_code == 405
        assert test_api.delete("/performers").status_code == 405
        assert test_api.put("/performers").status_code == 405

    def test_returns_list_of_valid_dicts(self, test_api):
        res = test_api.get("/performers")

        required_keys = ["performer_id", "performer_name",
                         "birth_year", "specialty_name"]

        data = res.json

        assert isinstance(data, list), "Not a list"
        assert all(isinstance(d, dict) for d in data), "Not a list of dicts"
        assert all(len(d.keys()) == len(required_keys)
                   for d in data), "Wrong number of keys"
        for k in required_keys:
            assert all(k in d for d in data), f"Key ({k}) not found in data"

    def test_returns_data_in_expected_order(self, test_api):
        """Checks that subjects are returned in descending order by birth year."""

        res = test_api.get("/performers")

        data = res.json

        years = [performer["birth_year"]
                for performer in data]

        for i in range(len(years) - 1):
            assert years[i] >= years[i + 1], "Subjects out of order!"

    def test_returns_data_with_expected_types(self, test_api):
        """Checks that the returned data has the expected types."""

        res = test_api.get("/performers")

        data = res.json

        for subject in data:

            for k, v in subject.items():
                if k not in ["performer_id", "birth_year"]:
                    assert isinstance(v, str)
                else:
                    assert isinstance(v, int)

    def test_returns_expected_data(self, test_api, example_performers):
        """Checks that the expected data is returned."""

        res = test_api.get("/performers")

        data = res.json

        assert len(data) == 50

        for i in range(len(data)):
            assert data[i] == example_performers[i]

    def test_returns_empty_list_if_no_subjects(self, test_api, test_temp_conn):
        """Checks that the response is an empty list if the database table is empty."""

        with test_temp_conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE performer CASCADE;")
            test_temp_conn.commit()

        res = test_api.get("/performers")

        data = res.json

        assert isinstance(data, list)
        assert len(data) == 0

    @pytest.mark.parametrize("sort_method", ('bread', 'wrong', 5, False, 'speciality_id', 'dob'))
    def test_incorrect_sort_method_returns_error(self, sort_method, test_api):
        res = test_api.get(f"/performers?sort={sort_method}")
        assert "error" in res.text
        assert "message" in res.text

    @pytest.mark.parametrize("order_method", ('desc', 'dusk', 5, False, 'ask', 'normal'))
    def test_incorrect_order_method_returns_error(self, order_method, test_api):
        res = test_api.get(f"/performers?order={order_method}")
        assert "error" in res.text
        assert "message" in res.text


class TestVenueRoute_Task_1:
    """Tests for the Performers routes"""

    def test_returns_200_status_code(self, test_api):
        res = test_api.get("/venues")
        assert res.status_code == 200

    def test_rejects_wrong_method_calls(self, test_api):
        """Tests that the api does not accept non GET methods"""
        assert test_api.post("/venues").status_code == 405
        assert test_api.patch("/venues").status_code == 405
        assert test_api.delete("/venues").status_code == 405
        assert test_api.put("/venues").status_code == 405

    def test_returns_list_of_valid_dicts(self, test_api):
        res = test_api.get("/venues")

        required_keys = ["venue_id", "venue_name"]

        data = res.json

        assert isinstance(data, list), "Not a list"
        assert all(isinstance(d, dict) for d in data), "Not a list of dicts"
        assert all(len(d.keys()) == len(required_keys)
                   for d in data), "Wrong number of keys"
        for k in required_keys:
            assert all(k in d for d in data), f"Key ({k}) not found in data"

    def test_returns_data_with_expected_types(self, test_api):
        """Checks that the returned data has the expected types."""

        res = test_api.get("/venues")

        data = res.json

        for subject in data:

            for k, v in subject.items():
                if k != "venue_id":
                    assert isinstance(v, str)
                else:
                    assert isinstance(v, int)

    def test_returns_expected_data(self, test_api, example_venues):
        """Checks that the expected data is returned."""

        res = test_api.get("/venues")

        data = res.json

        assert len(data) == 20

        for i in range(len(data)):
            assert data[i] == example_venues[i]

    def test_returns_empty_list_if_no_venues(self, test_api, test_temp_conn):
        """Checks that the response is an empty list if the database table is empty."""

        with test_temp_conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE venue CASCADE;")
            test_temp_conn.commit()

        res = test_api.get("/venues")

        data = res.json

        assert isinstance(data, list)
        assert len(data) == 0


class TestPerformancesRoute_Task_1:
    """Tests for the Performers routes"""

    def test_returns_200_status_code(self, test_api):
        res = test_api.get("/performances")
        assert res.status_code == 200

    def test_rejects_wrong_method_calls(self, test_api):
        """Tests that the api does not accept non GET methods"""
        assert test_api.patch("/performances").status_code == 405
        assert test_api.delete("/performances").status_code == 405
        assert test_api.put("/performances").status_code == 405

    def test_returns_list_of_valid_dicts(self, test_api):
        res = test_api.get("/performances")

        required_keys = ["performance_id",
                         "performer_name",
                         "performance_date",
                         "venue_name",
                         "score"]

        data = res.json

        assert isinstance(data, list), "Not a list"
        assert all(isinstance(d, dict) for d in data), "Not a list of dicts"
        assert all(len(d.keys()) == len(required_keys)
                   for d in data), "Wrong number of keys"
        for k in required_keys:
            assert all(k in d for d in data), f"Key ({k}) not found in data"

    def test_returns_data_with_expected_types(self, test_api):
        """Checks that the returned data has the expected types."""

        res = test_api.get("/performances")

        data = res.json

        for subject in data:

            for k, v in subject.items():
                if k not in ["performance_id", "score"]:
                    assert isinstance(v, str)
                else:
                    assert isinstance(v, int)

    def test_returns_empty_list_if_no_subjects(self, test_api, test_temp_conn):
        """Checks that the response is an empty list if the database table is empty."""

        with test_temp_conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE performance CASCADE;")
            test_temp_conn.commit()

        res = test_api.get("/performances")

        data = res.json

        assert isinstance(data, list)
        assert len(data) == 0

    def test_dates_are_in_correct_format(self, test_api):

        result = test_api.get("/performances")

        data = result.json

        for row in data:
            try:
                datetime.strptime(row["performance_date"], "%Y-%m-%d")
            except ValueError:
                assert False, "Date not in YYYY-MM-DD format"

class TestSinglePerformanceRoute_Task_1:
    """Tests for the Performers routes"""

    def test_returns_200_status_code(self, test_api):
        res = test_api.get("/performances/1")
        assert res.status_code == 200

    def test_rejects_wrong_method_calls(self, test_api):
        """Tests that the api does not accept non GET methods"""
        assert test_api.patch("/performances/1").status_code == 405
        assert test_api.delete("/performances/1").status_code == 405
        assert test_api.put("/performances/1").status_code == 405

    def test_returns_list_of_valid_dicts(self, test_api):
        res = test_api.get("/performances/1")

        required_keys = ["performance_id",
                         "performer_names",
                         "performance_date",
                         "venue_name",
                         "review_score"]

        data = res.json

        assert isinstance(data, dict), "Not a dict"
        assert all([key in data for key in required_keys])

    def test_returns_data_with_expected_types(self, test_api):
        """Checks that the returned data has the expected types."""

        res = test_api.get("/performances/1")

        data = res.json

        for k, v in data.items():
            if k in ["performance_id", "review_score"]:
                assert isinstance(v, int)
            elif k in ["performer_names"]:
                assert isinstance(v, list)
            else:
                assert isinstance(v, str)


    @pytest.mark.parametrize("n", (1, 2, 31, 9, 25, 4))
    def test_dates_are_in_correct_format(self, n, test_api):

        result = test_api.get(f"/performances/{n}")

        data = result.json
        try:
            datetime.strptime(data["performance_date"], "%Y-%m-%d")
        except ValueError:
            assert False, "Date not in YYYY-MM-DD format"


    def test_single_performance_returns_error_message_if_empty(self, test_api, test_temp_conn):
        with test_temp_conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE performance CASCADE;")
            test_temp_conn.commit()
        res = test_api.get("/performances/1")
        print(res)
        assert res.status_code == 404
        assert "error" in res.text


class TestPerformerSpecialtyRoute_Task_2:

    def test_returns_200_status_code(self, test_api):
        """Tests that the /performer_specialty endpoint returns a 200 status code"""
        res = test_api.get("/performer_specialty")
        assert res.status_code == 200

    def test_rejects_wrong_method_calls(self, test_api):
        """Tests that the api does not accept non-GET methods"""
        assert test_api.post("/performer_specialty").status_code == 405
        assert test_api.patch("/performer_specialty").status_code == 405
        assert test_api.delete("/performer_specialty").status_code == 405

    def test_returns_list_of_valid_dicts(self, test_api):
        """Tests that the /performer_specialty endpoint returns a list of valid dictionaries."""
        res = test_api.get("/performer_specialty")

        required_keys = ["specialty_id", "specialty_name", "performer_names"]

        data = res.json

        assert isinstance(data, list), "Response is not a list"
        assert all(isinstance(d, dict)
                   for d in data), "Response does not contain a list of dicts"
        assert all(len(d.keys()) == len(required_keys)
                   for d in data), "Incorrect number of keys in dictionary"
        for k in required_keys:
            assert all(k in d for d in data), f"Key ({k}) not found in data"

    def test_returns_data_with_expected_types(self, test_api):
        """Tests that the returned data has the expected types."""
        res = test_api.get("/performer_specialty")
        data = res.json

        for item in data:
            assert isinstance(item['specialty_id'],
                              int), "specialty_id is not an int"
            assert isinstance(item['specialty_name'],
                              str), "specialty_name is not a str"
            assert isinstance(item['performer_names'],
                              list), "performers is not a list"
            for performer in item['performer_names']:
                assert isinstance(
                    performer, str), "performer name is not a str"

    def test_returns_expected_data(self, test_api, example_performers):
        """Checks that the expected data is returned from the /performer_specialty endpoint."""
        res = test_api.get("/performer_specialty")
        data = res.json

        expected_specialties = {p['specialty_name']
                                for p in example_performers}
        returned_specialties = {s['specialty_name'] for s in data}

        assert expected_specialties.issubset(
            returned_specialties), "Returned specialties do not match expected data"

    def test_returns_empty_list_if_no_specialties(self, test_api, test_temp_conn):
        """Checks that the response is an empty list if the database table is empty."""
        with test_temp_conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE performer CASCADE;")
            test_temp_conn.commit()

        res = test_api.get("/performer_specialty")
        data = res.json

        assert isinstance(data, list), "Response is not a list"
        assert len(data) == 0, "List is not empty"


class TestPerformancesPost_Task_3:
    """Tests for the Performances POST route."""

    def test_returns_200_status_code(self, test_api):
        """Tests that the /performances POST endpoint processes correctly."""
        data = {
            "venue_name": "Grand Circus",
            "performer_id": [1, 2],
            "performance_date": "2024-01-01",
            "review_score": 85
        }
        res = test_api.post("/performances", json=data)
        assert res.status_code == 200

    def test_rejects_missing_data(self, test_api):
        """Tests rejection of incorrect or missing data."""
        data = {
            "venue_name": "Grand Circus",
            # Missing performer_id
            "performance_date": "2024-01-01",
            "review_score": 85
        }
        res = test_api.post("/performances", json=data)
        assert res.status_code == 400

    def test_handles_invalid_venue(self, test_api):
        """Tests how the API handles a nonexistent venue."""
        data = {
            "venue_name": "Nonexistent Venue",
            "performer_id": [1],
            "performance_date": "2024-01-01",
            "review_score": 85
        }
        res = test_api.post("/performances", json=data)
        assert res.status_code == 404
        assert "error" in res.text


class TestPerformersSummary_Task_4:
    """Tests for the Performers Summary route."""

    def test_returns_200_status_code(self, test_api):
        """Tests the /performers/summary endpoint returns 200 status code."""
        res = test_api.get("/performers/summary")
        assert res.status_code == 200

    def test_returns_list_of_valid_dicts(self, test_api):
        """Tests that the /performers/summary endpoint returns a list of valid dictionaries."""
        res = test_api.get("/performers/summary")

        required_keys = ["performer_id", "performer_stagename",
                         "total_performances", "average_review_score"]

        data = res.json

        assert isinstance(data, list), "Response is not a list"
        assert all(isinstance(d, dict)
                   for d in data), "Response does not contain a list of dicts"
        assert all(len(d.keys()) == len(required_keys)
                   for d in data), "Incorrect number of keys in dictionary"
        for k in required_keys:
            assert all(k in d for d in data), f"Key ({k}) not found in data"

    def test_returns_data_with_expected_types(self, test_api):
        """Checks that the returned data has the expected types."""
        res = test_api.get("/performers/summary")
        data = res.json

        for item in data:
            assert isinstance(item['performer_id'],
                              int), "performer_id is not an int"
            assert isinstance(item['performer_stagename'],
                              str), "performer_stagename is not a str"
            assert isinstance(item['total_performances'],
                              int), "total_performances is not an int"

    def test_returns_empty_list_if_no_data(self, test_api, test_temp_conn):
        """Checks that the response is an empty list if the database table is empty."""
        with test_temp_conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE performer CASCADE;")
            test_temp_conn.commit()

        res = test_api.get("/performers/summary")
        data = res.json

        assert isinstance(data, list), "Response is not a list"
        assert len(data) == 0, "List is not empty"
