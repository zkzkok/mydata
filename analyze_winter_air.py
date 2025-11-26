"""
Analyze winter air quality (AQI) for 2022-2024 station data and output ranking + map.
Requires only Python standard library.
"""
import csv
import datetime as dt
import glob
import json
import math
import os
from typing import Dict, List, Tuple

# Paths
DATA_DIRS = [
    "站点_20220101-20221231",
    "站点_20230101-20231231",
    "站点_20240101-20241231",
]
STATION_LIST_PATH = "站点列表-2022.02.13起.csv"
WINTER_MONTHS = {1, 2, 11, 12}
OUTPUT_MAP = "winter_air_quality_map.html"
OUTPUT_RANKING = "winter_aqi_ranking.csv"


def load_station_metadata(path: str) -> Dict[str, dict]:
    """Load station metadata (code -> info)."""

    def _to_float(text: str) -> float:
        try:
            return float(text)
        except (TypeError, ValueError):
            return 0.0

    stations = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("监测点编码")
            if not code:
                continue
            stations[code] = {
                "name": row.get("监测点名称", ""),
                "city": row.get("城市", "未知"),
                "lon": _to_float(row.get("经度", 0)),
                "lat": _to_float(row.get("纬度", 0)),
                "control": row.get("对照点", ""),
            }
    return stations


def list_data_files() -> List[str]:
    files = []
    for d in DATA_DIRS:
        if not os.path.isdir(d):
            continue
        files.extend(sorted(glob.glob(os.path.join(d, "china_sites_*.csv"))))
    return files


def read_header_sample(file_path: str) -> List[str]:
    with open(file_path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    return header


def parse_datetime(date_str: str, hour_str: str) -> dt.datetime:
    hour = int(hour_str)
    return dt.datetime.strptime(f"{date_str}{hour:02d}", "%Y%m%d%H")


def interpolate_series(values: List[float]) -> List[float]:
    result = values[:]
    n = len(result)
    i = 0
    while i < n:
        if result[i] is None:
            start = i
            while i < n and result[i] is None:
                i += 1
            end = i  # first non-None after the gap, or n if none
            prev_val = result[start - 1] if start > 0 else None
            next_val = result[end] if end < n else None
            if prev_val is None and next_val is None:
                # all missing; leave as None
                continue
            if prev_val is None:
                for j in range(start, end):
                    result[j] = next_val
                continue
            if next_val is None:
                for j in range(start, end):
                    result[j] = prev_val
                continue
            gap = end - start
            for offset in range(gap):
                ratio = (offset + 1) / (gap + 1)
                result[start + offset] = prev_val + (next_val - prev_val) * ratio
        else:
            i += 1
    return result


def build_time_series(station_codes: List[str], data_files: List[str]) -> Tuple[List[dt.datetime], Dict[str, List[float]]]:
    timestamps: List[dt.datetime] = []
    series: Dict[str, List[float]] = {code: [] for code in station_codes}

    for file_path in data_files:
        with open(file_path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            cols = header[3:]
            for row in reader:
                if len(row) < 3 or row[2] != "AQI":
                    continue
                ts = parse_datetime(row[0], row[1])
                if ts.month not in WINTER_MONTHS:
                    continue
                timestamps.append(ts)
                for idx, code in enumerate(cols):
                    if code not in series:
                        continue
                    value_str = row[3 + idx] if 3 + idx < len(row) else ""
                    if value_str.strip() == "":
                        value = None
                    else:
                        try:
                            value = float(value_str)
                        except ValueError:
                            value = None
                    series[code].append(value)
    return timestamps, series


def compute_station_means(series: Dict[str, List[float]]) -> Dict[str, float]:
    means = {}
    for code, values in series.items():
        cleaned = interpolate_series(values)
        numeric = [v for v in cleaned if v is not None]
        means[code] = sum(numeric) / len(numeric) if numeric else float("nan")
        series[code] = cleaned  # update with interpolated values
    return means


def aggregate_city_means(station_means: Dict[str, float], station_meta: Dict[str, dict]) -> Dict[str, float]:
    city_values: Dict[str, List[float]] = {}
    for code, mean_val in station_means.items():
        meta = station_meta.get(code)
        if not meta or math.isnan(mean_val):
            continue
        city = meta["city"]
        city_values.setdefault(city, []).append(mean_val)
    return {city: sum(vals) / len(vals) for city, vals in city_values.items() if vals}


def city_coordinates(station_meta: Dict[str, dict]) -> Dict[str, Tuple[float, float]]:
    coords: Dict[str, List[Tuple[float, float]]] = {}
    for meta in station_meta.values():
        city = meta["city"]
        coords.setdefault(city, []).append((meta["lat"], meta["lon"]))
    return {city: (sum(lat for lat, _ in lst) / len(lst), sum(lon for _, lon in lst) / len(lst)) for city, lst in coords.items() if lst}


def value_to_color(value: float, min_val: float, max_val: float) -> str:
    if max_val == min_val:
        return "#00ff00"
    ratio = (value - min_val) / (max_val - min_val)
    ratio = max(0.0, min(1.0, ratio))
    r = int(255 * ratio)
    g = int(255 * (1 - ratio))
    b = 0
    return f"#{r:02x}{g:02x}{b:02x}"


def save_ranking(city_means: Dict[str, float], path: str) -> None:
    ranking = sorted(city_means.items(), key=lambda x: x[1])
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["名次", "城市", "冬季平均AQI"])
        for idx, (city, mean_val) in enumerate(ranking, start=1):
            writer.writerow([idx, city, round(mean_val, 2)])


def generate_map(city_means: Dict[str, float], coords: Dict[str, Tuple[float, float]], path: str) -> None:
    ranking = sorted(city_means.items(), key=lambda x: x[1])
    values = [v for _, v in ranking]
    min_val, max_val = (min(values), max(values)) if values else (0, 1)
    features = []
    for city, mean_val in ranking:
        lat, lon = coords.get(city, (None, None))
        if lat is None or lon is None:
            continue
        features.append({
            "city": city,
            "aqi": round(mean_val, 2),
            "lat": lat,
            "lon": lon,
            "color": value_to_color(mean_val, min_val, max_val),
        })

    html = f"""<!DOCTYPE html>
<html lang=\"zh\">
<head>
  <meta charset=\"UTF-8\" />
  <title>冬季空气质量排行榜（2022-2024）</title>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" integrity=\"sha256-sA+vx6E1uu6wV6C0b8m2nLys9O6p3p0iJEJ4e5ihk54=\" crossorigin=\"\" />
  <style> #map {{ height: 700px; }} .legend {{ background: white; padding: 10px; line-height: 1.6; }} </style>
</head>
<body>
  <h2>2022-2024 年冬季（11/12/1/2 月）AQI 最佳城市</h2>
  <div id=\"map\"></div>
  <div class=\"legend\">颜色越绿表示 AQI 越低（空气越好），越红表示 AQI 越高。</div>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\" integrity=\"sha256-VHLoG2z8Xu1J10EhFM+w8ZRBK7f9BLeTYiQtohKQPe0=\" crossorigin=\"\"></script>
  <script>
    const map = L.map('map').setView([35, 105], 4);
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{ maxZoom: 18, attribution: '&copy; OpenStreetMap' }}).addTo(map);
    const cities = {json.dumps(features, ensure_ascii=False)};
    cities.forEach((item, idx) => {{
      const marker = L.circleMarker([item.lat, item.lon], {{
        radius: 7,
        color: item.color,
        fillColor: item.color,
        fillOpacity: 0.8,
        weight: 1
      }}).addTo(map);
      marker.bindPopup(`${{idx + 1}}. ${{item.city}}<br/>冬季平均AQI: ${{item.aqi}}`);
    }});
  </script>
</body>
</html>
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def main():
    station_meta = load_station_metadata(STATION_LIST_PATH)
    data_files = list_data_files()
    if not data_files:
        raise SystemExit("未找到数据文件，请确认数据目录存在")

    # Use first file header to determine station codes we care about
    header = read_header_sample(data_files[0])
    station_codes = [c for c in header[3:] if c in station_meta]

    timestamps, series = build_time_series(station_codes, data_files)
    print(f"收集到 {len(timestamps)} 个冬季小时数据点，共 {len(series)} 个站点")

    station_means = compute_station_means(series)
    city_means = aggregate_city_means(station_means, station_meta)
    coords = city_coordinates(station_meta)

    save_ranking(city_means, OUTPUT_RANKING)
    generate_map(city_means, coords, OUTPUT_MAP)

    print(f"城市数: {len(city_means)}")
    print(f"排行榜已保存到 {OUTPUT_RANKING}")
    print(f"地图已生成: {OUTPUT_MAP}")

    # 打印前 10 名
    ranking = sorted(city_means.items(), key=lambda x: x[1])
    print("前 10 名：")
    for idx, (city, val) in enumerate(ranking[:10], start=1):
        print(f"{idx}. {city} - 平均AQI: {val:.2f}")


if __name__ == "__main__":
    main()
