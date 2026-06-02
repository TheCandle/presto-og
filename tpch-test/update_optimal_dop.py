import csv
import math
from pathlib import Path


def transform_optimal_dop(value: str) -> str:
    """If optimal_dop != 1, divide by 4 and ceil when needed."""
    num = float(value)

    if num == 1:
        return "1"

    new_num = math.ceil(num / 4)

    # keep integer style output (e.g. 16 instead of 16.0)
    return str(int(new_num))


def main() -> None:
    input_path = Path(__file__).parent / "stage_threadblock_optimal_dop.csv"
    output_path = Path(__file__).parent / "stage_threadblock_optimal_dop.updated.csv"

    with input_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    if not fieldnames or "optimal_dop" not in fieldnames:
        raise ValueError("CSV 中未找到 optimal_dop 列")

    for row in rows:
        row["optimal_dop"] = transform_optimal_dop(row["optimal_dop"])

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"已生成: {output_path}")


if __name__ == "__main__":
    main()
