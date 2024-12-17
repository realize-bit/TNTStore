import sys
import statistics
from collections import defaultdict

def parse_file(file_name):
    try_counts = defaultdict(list)  # 시도 횟수를 키로 사용하여 시간을 저장

    with open(file_name, 'r') as file:
        for line in file:
            if "Try" not in line:  # "Try"가 포함된 줄만 처리
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            try_count = int(parts[0])
            time_ns = int(parts[-2])
            try_counts[try_count].append(time_ns)

    return try_counts

def calculate_statistics(try_counts):
    stats = {}
    for try_count, times in try_counts.items():
        mean_time = statistics.mean(times)
        median_time = statistics.median(times)
        percentile_99_time = statistics.quantiles(times, n=100)[98]  # 99th percentile
        stats[try_count] = {
            "count": len(times),
            "mean": mean_time,
            "median": median_time,
            "99p": percentile_99_time,
        }
    return stats

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <file_name>")
        return

    file_name = sys.argv[1]

    try_counts = parse_file(file_name)
    stats = calculate_statistics(try_counts)

    print("Summary of Try Counts:")
    for try_count, data in stats.items():
        print(f"Try {try_count}: {data['count']} occurrences, Mean: {data['mean']:.2f} ns, Median: {data['median']:.2f} ns, 99th Percentile: {data['99p']:.2f} ns")

if __name__ == "__main__":
    main()
