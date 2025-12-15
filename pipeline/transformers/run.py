import sys, json
from pipeline.transformers.factory import TransformerFactory

in_file = sys.argv[1]

with open(in_file) as f:
    extractor_data = json.load(f)

transformer = TransformerFactory.create("data")

output = transformer.transform(extractor_data)

print("\nFinal transformed output sample:")
print(output[:2])
