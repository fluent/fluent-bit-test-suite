from deepdiff import DeepDiff

def test_compare_jsons():
    json1 = {
        "name": "John",
        "age": 30,
        "city": "New York",
        "details": {
            "hobbies": ["reading", "traveling"],
            "education": {
                "degree": "Bachelor's",
                "university": "NYU"
            }
        }
    }

    json2 = {
        "name": "John",
        "age": 31,  # Different age
        "city": "New York",
        "details": {
            "hobbies": ["reading", "traveling"],
            "education": {
                "degree": "Bachelor's",
                "university": "NYU"
            }
        }
    }

    # Compare the two JSON objects
    diff = DeepDiff(json1, json2, ignore_order=True)

    # Assert there are no differences in specific fields
    fields_to_validate = ['name', 'city']
    for field in fields_to_validate:
        assert json1.get(field) == json2.get(field), f"Field '{field}' does not match: {json1.get(field)} != {json2.get(field)}"

    # Optionally, check that no other fields have changed (if required)
    assert not diff, f"Differences found: {diff}"

if __name__ == "__main__":
    test_compare_jsons()

