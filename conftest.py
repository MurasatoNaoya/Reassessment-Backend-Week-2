"""Fixtures used by multiple tests"""

# pylint: skip-file
import pytest

from api import app
from database_functions import get_connection


@pytest.fixture
def test_api():
    return app.test_client()


@pytest.fixture(autouse=True)
def setup_test_db():
    """Sets up a test database with the same structure as the real one."""

    conn = get_connection("postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS test_time_circus;")
        cur.execute("CREATE DATABASE test_time_circus;")
    conn.close()
    conn = get_connection("test_time_circus")
    conn.autocommit = True
    with conn.cursor() as cur:
        with open("setup-db.sql", 'r') as f:
            for q in f.read().split("\n\n"):
                cur.execute(q)
    conn.close()
    yield
    conn = get_connection("postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE test_time_circus WITH (FORCE);")
    conn.close()


@pytest.fixture(autouse=True)
def test_db_conn(monkeypatch):
    """Ensures that all tests use the test database."""
    mock_conn = get_connection("test_time_circus")
    monkeypatch.setattr("api.conn", mock_conn)


@pytest.fixture
def example_performers():
    return [
        {
            "birth_year": 6000,
            "performer_id": 22,
            "performer_name": "Cascadia",
            "specialty_name": "Magic"
        },
        {
            "birth_year": 5000,
            "performer_id": 21,
            "performer_name": "Chronos the Unseen",
            "specialty_name": "Tightrope Walking"
        },
        {
            "birth_year": 4100,
            "performer_id": 25,
            "performer_name": "Paradox Percy",
            "specialty_name": "Illusions"
        },
        {
            "birth_year": 4000,
            "performer_id": 14,
            "performer_name": "Time Turner",
            "specialty_name": "Clowning"
        },
        {
            "birth_year": 3200,
            "performer_id": 24,
            "performer_name": "Echo Enigma",
            "specialty_name": "Gravity Defying Acts"
        },
        {
            "birth_year": 3050,
            "performer_id": 50,
            "performer_name": "Clover Clown",
            "specialty_name": "Clowning"
        },
        {
            "birth_year": 3025,
            "performer_id": 37,
            "performer_name": "Aether Aiden",
            "specialty_name": "Escapology"
        },
        {
            "birth_year": 3020,
            "performer_id": 36,
            "performer_name": "Dream Dancer",
            "specialty_name": "Water Dancing"
        },
        {
            "birth_year": 3010,
            "performer_id": 35,
            "performer_name": "Stuntman Stan",
            "specialty_name": "Gravity Defying Acts"
        },
        {
            "birth_year": 3000,
            "performer_id": 13,
            "performer_name": "Infinite Ian",
            "specialty_name": "Animal Training"
        },
        {
            "birth_year": 2950,
            "performer_id": 30,
            "performer_name": "Quantum Quentin",
            "specialty_name": "Quantum Jumps"
        },
        {
            "birth_year": 2900,
            "performer_id": 31,
            "performer_name": "Whispering Willow",
            "specialty_name": "Animal Training"
        },
        {
            "birth_year": 2850,
            "performer_id": 33,
            "performer_name": "Kaleidoscope Kelly",
            "specialty_name": "Shadow Puppetry"
        },
        {
            "birth_year": 2750,
            "performer_id": 23,
            "performer_name": "Siren Sara",
            "specialty_name": "Water Dancing"
        },
        {
            "birth_year": 2750,
            "performer_id": 34,
            "performer_name": "Cinematic Callie",
            "specialty_name": "Unicycling"
        },
        {
            "birth_year": 2700,
            "performer_id": 47,
            "performer_name": "Mystery Mo",
            "specialty_name": "Mind Reading"
        },
        {
            "birth_year": 2605,
            "performer_id": 49,
            "performer_name": "Astro Ashley",
            "specialty_name": "Trapeze"
        },
        {
            "birth_year": 2600,
            "performer_id": 46,
            "performer_name": "Gadget Greg",
            "specialty_name": "Escapology"
        },
        {
            "birth_year": 2500,
            "performer_id": 40,
            "performer_name": "Warp Wayne",
            "specialty_name": "Quantum Jumps"
        },
        {
            "birth_year": 2450,
            "performer_id": 45,
            "performer_name": "Tidal Tilly",
            "specialty_name": "Tightrope Walking"
        },
        {
            "birth_year": 2400,
            "performer_id": 28,
            "performer_name": "Temporal Tanya",
            "specialty_name": "Knife Throwing"
        },
        {
            "birth_year": 2332,
            "performer_id": 48,
            "performer_name": "Electric Ella",
            "specialty_name": "Water Dancing"
        },
        {
            "birth_year": 2320,
            "performer_id": 44,
            "performer_name": "Spectacular Sienna",
            "specialty_name": "Magic"
        },
        {
            "birth_year": 2210,
            "performer_id": 43,
            "performer_name": "Plasma Percy",
            "specialty_name": "Fire Breathing"
        },
        {
            "birth_year": 2200,
            "performer_id": 12,
            "performer_name": "Galactic Gina",
            "specialty_name": "Trapeze"
        },
        {
            "birth_year": 2180,
            "performer_id": 27,
            "performer_name": "Holographic Hedley",
            "specialty_name": "Trapeze"
        },
        {
            "birth_year": 2140,
            "performer_id": 42,
            "performer_name": "Flexi Felix",
            "specialty_name": "Contortionism"
        },
        {
            "birth_year": 2100,
            "performer_id": 11,
            "performer_name": "Cosmic Chris",
            "specialty_name": "Quantum Jumps"
        },
        {
            "birth_year": 2090,
            "performer_id": 10,
            "performer_name": "Digital Dazzler",
            "specialty_name": "Gravity Defying Acts"
        },
        {
            "birth_year": 2085,
            "performer_id": 9,
            "performer_name": "Future Fiona",
            "specialty_name": "Illusions"
        },
        {
            "birth_year": 2055,
            "performer_id": 39,
            "performer_name": "Master Max",
            "specialty_name": "Juggling"
        },
        {
            "birth_year": 2050,
            "performer_id": 32,
            "performer_name": "Elastic Ethan",
            "specialty_name": "Contortionism"
        },
        {
            "birth_year": 2025,
            "performer_id": 17,
            "performer_name": "Eccentric Eloise",
            "specialty_name": "Shadow Puppetry"
        },
        {
            "birth_year": 2000,
            "performer_id": 29,
            "performer_name": "Psyche the Psychic",
            "specialty_name": "Mind Reading"
        },
        {
            "birth_year": 1994,
            "performer_id": 19,
            "performer_name": "Gritty Greta",
            "specialty_name": "Escapology"
        },
        {
            "birth_year": 1982,
            "performer_id": 18,
            "performer_name": "Bursting Barry",
            "specialty_name": "Contortionism"
        },
        {
            "birth_year": 1950,
            "performer_id": 16,
            "performer_name": "Time-Travelling Tom",
            "specialty_name": "Water Dancing"
        },
        {
            "birth_year": 1850,
            "performer_id": 41,
            "performer_name": "Psychic Phil",
            "specialty_name": "Mind Reading"
        },
        {
            "birth_year": 1800,
            "performer_id": 20,
            "performer_name": "Nimble Nick",
            "specialty_name": "Unicycling"
        },
        {
            "birth_year": 1500,
            "performer_id": 15,
            "performer_name": "Dimension Dan",
            "specialty_name": "Sword Swallowing"
        },
        {
            "birth_year": 1400,
            "performer_id": 8,
            "performer_name": "Gerald the Great",
            "specialty_name": "Fire Breathing"
        },
        {
            "birth_year": 1300,
            "performer_id": 7,
            "performer_name": "Arthur the Acrobatic",
            "specialty_name": "Acrobatics"
        },
        {
            "birth_year": 1250,
            "performer_id": 6,
            "performer_name": "Knight Knave",
            "specialty_name": "Knife Throwing"
        },
        {
            "birth_year": 1200,
            "performer_id": 38,
            "performer_name": "Vertigo Victor",
            "specialty_name": "Acrobatics"
        },
        {
            "birth_year": 1100,
            "performer_id": 5,
            "performer_name": "Merlin the Marvelous",
            "specialty_name": "Magic"
        },
        {
            "birth_year": 920,
            "performer_id": 26,
            "performer_name": "Pyro Petra",
            "specialty_name": "Fire Breathing"
        },
        {
            "birth_year": 150,
            "performer_id": 4,
            "performer_name": "Atlas the Strongman",
            "specialty_name": "Strongman"
        },
        {
            "birth_year": 123,
            "performer_id": 1,
            "performer_name": "Orac the Oracle",
            "specialty_name": "Mind Reading"
        },
        {
            "birth_year": 100,
            "performer_id": 2,
            "performer_name": "Julius the Juggler",
            "specialty_name": "Juggling"
        },
        {
            "birth_year": 95,
            "performer_id": 3,
            "performer_name": "Zephyra the Zesty",
            "specialty_name": "Acrobatics"
        }
    ]


@pytest.fixture
def example_venues():
    return [
        {
            "venue_id": 1,
            "venue_name": "Grand Circus"
        },
        {
            "venue_id": 2,
            "venue_name": "Roman Colosseum"
        },
        {
            "venue_id": 3,
            "venue_name": "Egyptian Pyramid"
        },
        {
            "venue_id": 4,
            "venue_name": "Grand Central Terminal"
        },
        {
            "venue_id": 5,
            "venue_name": "Ancient Greece Amphitheatre"
        },
        {
            "venue_id": 6,
            "venue_name": "Medieval Castle Grounds"
        },
        {
            "venue_id": 7,
            "venue_name": "Victorian Exhibition Hall"
        },
        {
            "venue_id": 8,
            "venue_name": "Renaissance Festival"
        },
        {
            "venue_id": 9,
            "venue_name": "Futuristic Dome"
        },
        {
            "venue_id": 10,
            "venue_name": "Space Station Arena"
        },
        {
            "venue_id": 11,
            "venue_name": "Underwater Stage"
        },
        {
            "venue_id": 12,
            "venue_name": "Jungle Clearing"
        },
        {
            "venue_id": 13,
            "venue_name": "Desert Oasis"
        },
        {
            "venue_id": 14,
            "venue_name": "Mountain Top"
        },
        {
            "venue_id": 15,
            "venue_name": "Beachfront Pavilion"
        },
        {
            "venue_id": 16,
            "venue_name": "Mars Amphitheatre"
        },
        {
            "venue_id": 17,
            "venue_name": "Venus Sky Dome"
        },
        {
            "venue_id": 18,
            "venue_name": "Atlantis Arena"
        },
        {
            "venue_id": 19,
            "venue_name": "Galactic Theater"
        },
        {
            "venue_id": 20,
            "venue_name": "Time Travel Hub"
        }
    ]


@pytest.fixture
def example_performance():
    return [
        {
            "performance_date": "6000-03-03",
            "performance_id": 16,
            "performer_name": "Chronos the Unseen",
            "score": 94,
            "venue_name": "Mars Amphitheatre"
        },
        {
            "performance_date": "6000-03-03",
            "performance_id": 16,
            "performer_name": "Time Turner",
            "score": 94,
            "venue_name": "Mars Amphitheatre"
        },
        {
            "performance_date": "5500-08-08",
            "performance_id": 17,
            "performer_name": "Dimension Dan",
            "score": 97,
            "venue_name": "Venus Sky Dome"
        },
        {
            "performance_date": "5500-08-08",
            "performance_id": 17,
            "performer_name": "Cascadia",
            "score": 97,
            "venue_name": "Venus Sky Dome"
        },
        {
            "performance_date": "5000-12-31",
            "performance_id": 15,
            "performer_name": "Merlin the Marvelous",
            "score": 85,
            "venue_name": "Beachfront Pavilion"
        },
        {
            "performance_date": "5000-12-31",
            "performance_id": 15,
            "performer_name": "Infinite Ian",
            "score": 85,
            "venue_name": "Beachfront Pavilion"
        },
        {
            "performance_date": "4832-09-25",
            "performance_id": 32,
            "performer_name": "Aether Aiden",
            "score": 96,
            "venue_name": "Jungle Clearing"
        },
        {
            "performance_date": "4500-06-17",
            "performance_id": 22,
            "performer_name": "Holographic Hedley",
            "score": 91,
            "venue_name": "Roman Colosseum"
        },
        {
            "performance_date": "4500-06-17",
            "performance_id": 22,
            "performer_name": "Mystery Mo",
            "score": 91,
            "venue_name": "Roman Colosseum"
        },
        {
            "performance_date": "4000-09-09",
            "performance_id": 20,
            "performer_name": "Bursting Barry",
            "score": 88,
            "venue_name": "Time Travel Hub"
        },
        {
            "performance_date": "4000-09-09",
            "performance_id": 20,
            "performer_name": "Paradox Percy",
            "score": 88,
            "venue_name": "Time Travel Hub"
        },
        {
            "performance_date": "3999-06-18",
            "performance_id": 31,
            "performer_name": "Dream Dancer",
            "score": 86,
            "venue_name": "Underwater Stage"
        },
        {
            "performance_date": "3899-12-19",
            "performance_id": 29,
            "performer_name": "Cinematic Callie",
            "score": 91,
            "venue_name": "Futuristic Dome"
        },
        {
            "performance_date": "3500-03-21",
            "performance_id": 23,
            "performer_name": "Temporal Tanya",
            "score": 93,
            "venue_name": "Egyptian Pyramid"
        },
        {
            "performance_date": "3500-03-21",
            "performance_id": 23,
            "performer_name": "Electric Ella",
            "score": 93,
            "venue_name": "Egyptian Pyramid"
        },
        {
            "performance_date": "3230-03-11",
            "performance_id": 33,
            "performer_name": "Vertigo Victor",
            "score": 88,
            "venue_name": "Desert Oasis"
        },
        {
            "performance_date": "3000-03-03",
            "performance_id": 19,
            "performer_name": "Eccentric Eloise",
            "score": 86,
            "venue_name": "Galactic Theater"
        },
        {
            "performance_date": "3000-03-03",
            "performance_id": 19,
            "performer_name": "Echo Enigma",
            "score": 86,
            "venue_name": "Galactic Theater"
        },
        {
            "performance_date": "3000-01-01",
            "performance_id": 11,
            "performer_name": "Gritty Greta",
            "score": 100,
            "venue_name": "Underwater Stage"
        },
        {
            "performance_date": "3000-01-01",
            "performance_id": 11,
            "performer_name": "Future Fiona",
            "score": 100,
            "venue_name": "Underwater Stage"
        },
        {
            "performance_date": "2999-11-30",
            "performance_id": 27,
            "performer_name": "Elastic Ethan",
            "score": 98,
            "venue_name": "Victorian Exhibition Hall"
        },
        {
            "performance_date": "2899-07-08",
            "performance_id": 30,
            "performer_name": "Stuntman Stan",
            "score": 79,
            "venue_name": "Space Station Arena"
        },
        {
            "performance_date": "2750-02-14",
            "performance_id": 28,
            "performer_name": "Kaleidoscope Kelly",
            "score": 85,
            "venue_name": "Renaissance Festival"
        },
        {
            "performance_date": "2500-07-11",
            "performance_id": 13,
            "performer_name": "Cosmic Chris",
            "score": 90,
            "venue_name": "Desert Oasis"
        },
        {
            "performance_date": "2500-07-11",
            "performance_id": 13,
            "performer_name": "Orac the Oracle",
            "score": 90,
            "venue_name": "Desert Oasis"
        },
        {
            "performance_date": "2323-11-27",
            "performance_id": 34,
            "performer_name": "Master Max",
            "score": 90,
            "venue_name": "Mountain Top"
        },
        {
            "performance_date": "2300-04-22",
            "performance_id": 26,
            "performer_name": "Whispering Willow",
            "score": 90,
            "venue_name": "Medieval Castle Grounds"
        },
        {
            "performance_date": "2100-01-23",
            "performance_id": 35,
            "performer_name": "Warp Wayne",
            "score": 89,
            "venue_name": "Beachfront Pavilion"
        },
        {
            "performance_date": "2024-07-04",
            "performance_id": 10,
            "performer_name": "Gerald the Great",
            "score": 97,
            "venue_name": "Space Station Arena"
        },
        {
            "performance_date": "2024-06-21",
            "performance_id": 9,
            "performer_name": "Arthur the Acrobatic",
            "score": 89,
            "venue_name": "Futuristic Dome"
        },
        {
            "performance_date": "2024-05-05",
            "performance_id": 8,
            "performer_name": "Knight Knave",
            "score": 93,
            "venue_name": "Renaissance Festival"
        },
        {
            "performance_date": "2024-04-01",
            "performance_id": 7,
            "performer_name": "Merlin the Marvelous",
            "score": 87,
            "venue_name": "Victorian Exhibition Hall"
        },
        {
            "performance_date": "2024-03-17",
            "performance_id": 6,
            "performer_name": "Julius the Juggler",
            "score": 91,
            "venue_name": "Medieval Castle Grounds"
        },
        {
            "performance_date": "2024-02-14",
            "performance_id": 5,
            "performer_name": "Orac the Oracle",
            "score": 88,
            "venue_name": "Ancient Greece Amphitheatre"
        },
        {
            "performance_date": "2024-01-01",
            "performance_id": 1,
            "performer_name": "Orac the Oracle",
            "score": 95,
            "venue_name": "Grand Circus"
        },
        {
            "performance_date": "2023-12-25",
            "performance_id": 2,
            "performer_name": "Julius the Juggler",
            "score": 90,
            "venue_name": "Roman Colosseum"
        },
        {
            "performance_date": "2023-11-30",
            "performance_id": 3,
            "performer_name": "Zephyra the Zesty",
            "score": 85,
            "venue_name": "Egyptian Pyramid"
        },
        {
            "performance_date": "2023-10-15",
            "performance_id": 4,
            "performer_name": "Atlas the Strongman",
            "score": 92,
            "venue_name": "Grand Central Terminal"
        },
        {
            "performance_date": "2000-09-09",
            "performance_id": 18,
            "performer_name": "Time-Travelling Tom",
            "score": 91,
            "venue_name": "Atlantis Arena"
        },
        {
            "performance_date": "2000-09-09",
            "performance_id": 18,
            "performer_name": "Siren Sara",
            "score": 91,
            "venue_name": "Atlantis Arena"
        },
        {
            "performance_date": "1800-10-20",
            "performance_id": 14,
            "performer_name": "Galactic Gina",
            "score": 88,
            "venue_name": "Mountain Top"
        },
        {
            "performance_date": "1800-10-20",
            "performance_id": 14,
            "performer_name": "Zephyra the Zesty",
            "score": 88,
            "venue_name": "Mountain Top"
        },
        {
            "performance_date": "1515-05-05",
            "performance_id": 40,
            "performer_name": "Tidal Tilly",
            "score": 93,
            "venue_name": "Time Travel Hub"
        },
        {
            "performance_date": "1500-05-15",
            "performance_id": 12,
            "performer_name": "Digital Dazzler",
            "score": 92,
            "venue_name": "Jungle Clearing"
        },
        {
            "performance_date": "1500-05-15",
            "performance_id": 12,
            "performer_name": "Nimble Nick",
            "score": 92,
            "venue_name": "Jungle Clearing"
        },
        {
            "performance_date": "1450-08-08",
            "performance_id": 25,
            "performer_name": "Quantum Quentin",
            "score": 84,
            "venue_name": "Ancient Greece Amphitheatre"
        },
        {
            "performance_date": "1450-08-08",
            "performance_id": 25,
            "performer_name": "Clover Clown",
            "score": 84,
            "venue_name": "Ancient Greece Amphitheatre"
        },
        {
            "performance_date": "1414-04-04",
            "performance_id": 39,
            "performer_name": "Spectacular Sienna",
            "score": 85,
            "venue_name": "Galactic Theater"
        },
        {
            "performance_date": "1313-10-10",
            "performance_id": 38,
            "performer_name": "Plasma Percy",
            "score": 87,
            "venue_name": "Atlantis Arena"
        },
        {
            "performance_date": "1212-08-12",
            "performance_id": 37,
            "performer_name": "Flexi Felix",
            "score": 92,
            "venue_name": "Venus Sky Dome"
        },
        {
            "performance_date": "1200-09-15",
            "performance_id": 24,
            "performer_name": "Psyche the Psychic",
            "score": 87,
            "venue_name": "Grand Central Terminal"
        },
        {
            "performance_date": "1200-09-15",
            "performance_id": 24,
            "performer_name": "Astro Ashley",
            "score": 87,
            "venue_name": "Grand Central Terminal"
        },
        {
            "performance_date": "1111-07-07",
            "performance_id": 36,
            "performer_name": "Psychic Phil",
            "score": 83,
            "venue_name": "Mars Amphitheatre"
        },
        {
            "performance_date": "1000-01-01",
            "performance_id": 21,
            "performer_name": "Pyro Petra",
            "score": 82,
            "venue_name": "Grand Circus"
        },
        {
            "performance_date": "1000-01-01",
            "performance_id": 21,
            "performer_name": "Gadget Greg",
            "score": 82,
            "venue_name": "Grand Circus"
        }
    ]


@pytest.fixture
def test_temp_conn():
    return get_connection("test_time_circus")
