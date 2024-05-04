import os
import re
import json
from collections import defaultdict


def parse_log_line(line):
    pattern = r'(\S+) - - \[(.+)\] "(\S+) (\S+)\s?(\S+)?\s?(\S+)?" (\d+) (\d+) "(.+)" "(.+)" (\d+)'
    match = re.match(pattern, line)
    if match:
        ip, timestamp, method, url, protocol, _, status, bytes_sent, referer, user_agent, duration = match.groups()
        return {
            'ip': ip,
            'timestamp': timestamp,
            'method': method,
            'url': url,
            'protocol': protocol,
            'status': int(status),
            'bytes_sent': int(bytes_sent),
            'referer': referer,
            'user_agent': user_agent,
            'duration': int(duration)
        }
    return None


def process_log_files(log_path):
    total_requests = 0
    method_counts = defaultdict(int)
    ip_counts = defaultdict(int)
    longest_requests = []

    if os.path.isfile(log_path):
        log_files = [log_path]
    else:
        log_files = [os.path.join(log_path, f) for f in os.listdir(log_path) if f.endswith('.log')]

    for log_file in log_files:
        with open(log_file, 'r') as file:
            for line in file:
                log_data = parse_log_line(line)
                if log_data:
                    total_requests += 1
                    method_counts[log_data['method']] += 1
                    ip_counts[log_data['ip']] += 1

                    longest_requests.append({
                        'ip': log_data['ip'],
                        'date': log_data['timestamp'],
                        'method': log_data['method'],
                        'url': log_data['url'],
                        'duration': log_data['duration']
                    })
                    longest_requests.sort(key=lambda x: x['duration'], reverse=True)
                    longest_requests = longest_requests[:3]

        top_ips = sorted(ip_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        top_ips = {ip: count for ip, count in top_ips}

        log_stats = {
            'top_ips': top_ips,
            'top_longest': longest_requests,
            'total_stat': dict(method_counts),
            'total_requests': total_requests
        }

        log_file_name = os.path.basename(log_file)
        json_file_name = os.path.splitext(log_file_name)[0] + '.json'
        with open(json_file_name, 'w') as json_file:
            json.dump(log_stats, json_file, indent=2)

        print(f"Статистика для файла: {log_file_name}")
        print(json.dumps(log_stats, indent=2))
        print()


process_log_files('/home/andrew/Загрузки/access.log')
