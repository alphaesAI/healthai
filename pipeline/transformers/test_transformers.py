"""
Simple manual test runner for transformer layer.

Validates that:
1. DataTransformer loads correctly
2. Textractor integration works
3. Output is txtai-ready

Run:
python -m pipeline.transformers.test_transformers
"""

import os
from pipeline.transformers.factory import TransformerFactory


def mock_extractor_output():
    """
    Fake extractor output to simulate Gmail extractor.
    """
    return [
        {
            "metadata": {
                "id": "msg_001",
                "subject": "Test Mail",
                "from": "alice@example.com",
                "to": "bob@example.com",
                "date": "2025-01-01",
            },
            "html": "<p>Hello Logi, this is a test email.</p>",
            "attachments": [],
        }
    ]


def test_data_transformer():
    print("\n[TEST] DataTransformer")

    transformer = TransformerFactory.create(
        transformer_type="data",
        name="data_transformer",
        config={}
    )

    input_data = mock_extractor_output()
    output = transformer.transform(input_data)

    assert isinstance(output, list)
    assert len(output) > 0

    sample = output[0]
    assert "id" in sample
    assert "text" in sample
    assert "tags" in sample

    print("Sample output:", sample)
    print("[PASS] DataTransformer")


if __name__ == "__main__":
    print("Starting transformer tests...")

    test_data_transformer()

    print("\nAll transformer tests completed successfully.")
