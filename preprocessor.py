import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

KPA_TO_PSI = 6.895

def parse_json(msg_bytes: bytes) -> dict:
    return json.loads(msg_bytes.decode("utf-8"))

def is_complete(record: dict) -> bool:
    return (
        record.get("temperature") is not None
        and record.get("humidity") is not None
        and record.get("pressure") is not None
    )

def convert_units(record: dict) -> dict:
    # pressure: kPa -> psi
    record["pressure"] = float(record["pressure"]) / KPA_TO_PSI
    # temperature: C -> F
    record["temperature"] = float(record["temperature"]) * 1.8 + 32.0
    return record

def to_json_bytes(record: dict) -> bytes:
    return json.dumps(record).encode("utf-8")

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Pub/Sub topic: projects/<id>/topics/<topic>")
    parser.add_argument("--output", required=True, help="Pub/Sub topic: projects/<id>/topics/<topic>")
    args, beam_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=args.input)
            | "ParseJson" >> beam.Map(parse_json)
            | "FilterMissing" >> beam.Filter(is_complete)
            | "ConvertUnits" >> beam.Map(convert_units)
            | "ToJsonBytes" >> beam.Map(to_json_bytes)
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=args.output)
        )

if __name__ == "__main__":
    run()
