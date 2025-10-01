#!/usr/bin/env python3

import asyncio
import aiohttp
import time
import json
import os
import tempfile
from typing import List, Tuple, Dict, Optional, Union
import sys
import argparse
import ipaddress
import urllib.parse

DEFAULT_TEST_URL = "https://1.1.1.1/cdn-cgi/trace"

def validate_proxy(proxy: str) -> bool:
    """Validate proxy format: host:port (host can be IPv4, IPv6 in brackets, or domain)"""
    if not proxy or ':' not in proxy:
        return False
    # Handle IPv6: [::1]:8080
    if proxy.startswith('['):
        if ']' not in proxy:
            return False
        host_part, _, port_part = proxy.partition(']:')
        if not host_part or not port_part:
            return False
        host = host_part[1:]  # remove leading '['
        port_str = port_part
    else:
        parts = proxy.rsplit(':', 1)  # split on last colon (in case host has colons, e.g., IPv6 without brackets)
        if len(parts) != 2:
            return False
        host, port_str = parts

    host = host.strip()
    port_str = port_str.strip()
    if not host or not port_str:
        return False

    try:
        port = int(port_str)
        if not (1 <= port <= 65535):
            return False
    except ValueError:
        return False

    # Basic host validation: allow domains, IPv4, IPv6 (without brackets here)
    if host == 'localhost':
        return True
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        pass

    # Simple domain check (not exhaustive)
    if all(c.isalnum() or c in ('.', '-', '_') for c in host) and len(host) <= 253:
        return True

    return False


def is_ip_in_cidr_list(ip_str: str, cidr_list: List[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        for cidr in cidr_list:
            if ip in cidr:
                return True
    except ValueError:
        pass
    return False


def extract_host_from_proxy(proxy: str) -> Optional[str]:
    """Extract host from proxy string (handle IPv6 in brackets)"""
    if proxy.startswith('['):
        end_bracket = proxy.find(']')
        if end_bracket == -1:
            return None
        return proxy[1:end_bracket]
    else:
        host = proxy.split(':')[0].strip()
        return host if host else None


def filter_proxies_by_cidr(proxies: List[str], skip_cidr_list: List[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]) -> List[str]:
    if not skip_cidr_list:
        return proxies

    filtered = []
    for proxy in proxies:
        host = extract_host_from_proxy(proxy)
        if host is None:
            filtered.append(proxy)  # keep if can't parse (let test fail later)
            continue
        if not is_ip_in_cidr_list(host, skip_cidr_list):
            filtered.append(proxy)
    return filtered


def read_cidr_list_from_file(filename: str) -> List[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]:
    try:
        cidrs = []
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    try:
                        network = ipaddress.ip_network(line, strict=False)
                        cidrs.append(network)
                    except ValueError as e:
                        print(f"Warning: Invalid CIDR in skip file '{filename}': {line} ({e})")
        return cidrs
    except FileNotFoundError:
        print(f"Error: Skip CIDR file '{filename}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to read skip CIDR file '{filename}' - {e}")
        sys.exit(1)


def extract_loc_from_trace(response_text: str) -> Optional[str]:
    """Extract 'loc=XX' from trace response."""
    for line in response_text.splitlines():
        if line.startswith('loc='):
            return line[4:].strip() or None
    return None


async def test_http_proxy(
    proxy: str,
    session: aiohttp.ClientSession,
    timeout: int,
    test_url: str
) -> Tuple[str, bool, float, str, Optional[str]]:
    start_time = time.time()
    try:
        http_proxy_url = f"http://{proxy}"
        async with session.get(
            test_url,
            proxy=http_proxy_url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            ssl=False  # Disable SSL verification for proxy testing (optional, but common)
        ) as response:
            response_time = time.time() - start_time
            response_text = await response.text()

            if response.status == 200:
                loc = extract_loc_from_trace(response_text)
                return proxy, True, response_time, "", loc
            else:
                return proxy, False, response_time, f"HTTP {response.status}", None

    except asyncio.TimeoutError:
        response_time = time.time() - start_time
        return proxy, False, response_time, "Timeout", None
    except aiohttp.ClientProxyConnectionError:
        response_time = time.time() - start_time
        return proxy, False, response_time, "Proxy connection failed", None
    except aiohttp.ClientHttpProxyError:
        response_time = time.time() - start_time
        return proxy, False, response_time, "Proxy HTTP error", None
    except aiohttp.ClientSSLError:
        response_time = time.time() - start_time
        return proxy, False, response_time, "SSL/TLS error", None
    except aiohttp.ClientConnectorError:
        response_time = time.time() - start_time
        return proxy, False, response_time, "Connection failed", None
    except Exception:
        response_time = time.time() - start_time
        return proxy, False, response_time, "Unknown error", None


async def test_all_proxies(
    proxies: List[str],
    timeout: int,
    max_concurrent: int,
    test_url: str
) -> List[Tuple[str, bool, float, str, Optional[str]]]:
    if not proxies:
        return []
    connector = aiohttp.TCPConnector(
        limit=max_concurrent,
        limit_per_host=10,
        force_close=True,
        enable_cleanup_closed=True
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [test_http_proxy(proxy.strip(), session, timeout, test_url) for proxy in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append((proxies[i].strip(), False, 0.0, f"Exception: {str(result)}", None))
            else:
                processed_results.append(result)
        return processed_results


def read_proxies_from_file(filename: str) -> List[str]:
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        valid_proxies = []
        invalid_count = 0
        for line in lines:
            if validate_proxy(line):
                valid_proxies.append(line)
            else:
                invalid_count += 1
        if invalid_count:
            print(f"Warning: Skipped {invalid_count} invalid proxy entries")
        return valid_proxies
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to read file - {e}")
        sys.exit(1)


def load_timestamps(json_file: str) -> Dict[str, Dict[str, Union[float, str, None]]]:
    """Load proxy info: {proxy: {'added_at': ts, 'location': 'XX'}}"""
    if not os.path.exists(json_file):
        return {}
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            cleaned = {}
            for proxy, info in data.items():
                if isinstance(info, dict) and 'added_at' in info:
                    cleaned[proxy] = {
                        'added_at': float(info['added_at']),
                        'location': info.get('location')
                    }
            return cleaned
    except Exception as e:
        print(f"Warning: Failed to load timestamp file '{json_file}' - {e}")
        return {}


def save_timestamps(proxy_info: Dict[str, Dict[str, Union[float, str, None]]], json_file: str):
    """Save proxy info to JSON atomically."""
    try:
        dir_path = os.path.dirname(json_file) or '.'
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, dir=dir_path, suffix='.tmp') as f:
            json.dump(proxy_info, f, indent=2, sort_keys=True, ensure_ascii=False)
            temp_name = f.name
        os.replace(temp_name, json_file)
    except Exception as e:
        print(f"Warning: Failed to save timestamp file '{json_file}' - {e}")


def save_results_to_file(results: List[Tuple[str, bool, float, str, Optional[str]]], filename: str, proxy_info: Dict[str, Dict[str, Union[float, str, None]]]):
    available_proxies = [proxy for proxy, ok, _, _, _ in results if ok]

    def sort_key(proxy):
        info = proxy_info.get(proxy, {})
        ts = info.get('added_at', float('inf'))
        return ts

    sorted_proxies = sorted(available_proxies, key=sort_key)

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for proxy in sorted_proxies:
                f.write(f"{proxy}\n")
        print(f"Available proxies saved to: {filename} (sorted by addition time)")
    except Exception as e:
        print(f"Warning: Failed to save results file - {e}")


def print_results(results: List[Tuple[str, bool, float, str, Optional[str]]], round_num: int = None):
    available_count = sum(1 for r in results if r[1])
    total_count = len(results)

    header = f"\n{'='*80}"
    if round_num is not None:
        header += f"\nRound {round_num} Test Results"
    header += f"\nTest Results (Total: {total_count} proxies)"
    header += f"\n{'='*80}"
    print(header)

    for proxy, is_available, response_time, error, loc in results:
        if is_available:
            time_str = f"{response_time:.2f}s"
            loc_str = loc if loc else "N/A"
            print(f"{proxy:<25} | {time_str:<8} | loc={loc_str}")

    print(f"{'='*80}")
    success_rate = (available_count / total_count * 100) if total_count > 0 else 0
    print(f"Total: {total_count} proxies, Available: {available_count}, Unavailable: {total_count - available_count}")
    print(f"Success Rate: {success_rate:.1f}%")


def main():
    parser = argparse.ArgumentParser(description='Test HTTP proxy availability with location tracking')
    parser.add_argument('input_file', help='Input file containing HTTP proxy list (host:port)')
    parser.add_argument('-o', '--output', help='Output file to save final available proxies')
    parser.add_argument('-t', '--timeout', type=int, default=5, help='Timeout in seconds (default: 5)')
    parser.add_argument('-c', '--concurrent', type=int, default=70, help='Max concurrent connections (default: 70)')
    parser.add_argument('--max-rounds', type=int, default=30, help='Max test rounds (default: 30)')
    parser.add_argument('--skip-cidr', help='File containing CIDR ranges to skip (one per line)')
    parser.add_argument('--test-url', default=DEFAULT_TEST_URL,
                        help=f'Health check URL (default: {DEFAULT_TEST_URL})')

    args = parser.parse_args()

    timestamp_file = None
    if args.output:
        base_name = os.path.splitext(args.output)[0]
        timestamp_file = base_name + '.json'

    proxy_info = {}
    if timestamp_file:
        proxy_info = load_timestamps(timestamp_file)
        print(f"Loaded {len(proxy_info)} proxy records from '{timestamp_file}'")

    print(f"Reading proxy list from file '{args.input_file}'...")
    raw_proxies = read_proxies_from_file(args.input_file)

    if not raw_proxies:
        print("Error: Proxy list is empty")
        sys.exit(1)

    unique_proxies = list(dict.fromkeys(raw_proxies))
    if len(unique_proxies) != len(raw_proxies):
        print(f"Deduplicated: {len(raw_proxies)} → {len(unique_proxies)} proxies")

    skip_cidr_list = []
    if args.skip_cidr:
        print(f"Loading CIDR skip list from '{args.skip_cidr}'...")
        skip_cidr_list = read_cidr_list_from_file(args.skip_cidr)
        print(f"Loaded {len(skip_cidr_list)} CIDR ranges to skip")

    current_proxies = filter_proxies_by_cidr(unique_proxies, skip_cidr_list)
    if len(current_proxies) != len(unique_proxies):
        print(f"After CIDR filtering: {len(current_proxies)} proxies remain")

    if not current_proxies:
        print("Error: No proxies left after deduplication and CIDR filtering")
        sys.exit(1)

    print(f"Starting multi-round stability test with {len(current_proxies)} proxies...")

    consecutive_success = 0
    round_num = 0
    current_run_start_time = time.time()

    try:
        while consecutive_success < 3 and round_num < args.max_rounds:
            round_num += 1
            print(f"\n>>> Starting Round {round_num} (Current consecutive success: {consecutive_success})")

            results = asyncio.run(test_all_proxies(current_proxies, args.timeout, args.concurrent, args.test_url))
            print_results(results, round_num)

            available_proxies_with_loc = [(proxy, loc) for proxy, ok, _, _, loc in results if ok]
            available_proxies = [p for p, _ in available_proxies_with_loc]
            total = len(current_proxies)

            if total == 0:
                print("Error: Current proxy list is empty, terminating test")
                break

            if timestamp_file:
                for proxy, loc in available_proxies_with_loc:
                    if proxy not in proxy_info:
                        proxy_info[proxy] = {
                            'added_at': current_run_start_time,
                            'location': loc or "N/A"
                        }

            if len(available_proxies) == total:
                consecutive_success += 1
                print(f"Round {round_num} passed 100%! Consecutive success: {consecutive_success}/3")
            else:
                consecutive_success = 0
                print(f"Round {round_num} did not pass 100%, resetting consecutive counter")
                current_proxies = available_proxies

            if not available_proxies:
                print("No proxies passed this round, terminating early")
                break

        if consecutive_success >= 3:
            print("\nPassed 3 consecutive rounds with 100% success! Testing complete.")
        else:
            print(f"\nDid not achieve 3 consecutive 100% passes (max rounds {args.max_rounds} reached or no proxies left)")

        if args.output and current_proxies:
            print("Re-testing final proxy list for up-to-date status...")
            final_results = asyncio.run(test_all_proxies(current_proxies, args.timeout, args.concurrent, args.test_url))
            available_final = [r for r in final_results if r[1]]
            available_final_proxies = [proxy for proxy, _, _, _, _ in available_final]

            if available_final_proxies:
                cleaned_info = {}
                now = time.time()
                for proxy, _, _, _, loc in available_final:
                    if proxy in proxy_info:
                        cleaned_info[proxy] = proxy_info[proxy]
                    else:
                        cleaned_info[proxy] = {
                            'added_at': now,
                            'location': loc or "N/A"
                        }

                if timestamp_file:
                    save_timestamps(cleaned_info, timestamp_file)

                save_results_to_file(final_results, args.output, cleaned_info)
            else:
                print("Warning: No proxies available in final test, skipping output file")
                if timestamp_file and os.path.exists(timestamp_file):
                    try:
                        os.remove(timestamp_file)
                        print(f"Removed empty timestamp file: {timestamp_file}")
                    except Exception as e:
                        print(f"Warning: Failed to remove empty timestamp file - {e}")
        elif args.output:
            print("Warning: No proxies to save, skipping output file")
            if timestamp_file and os.path.exists(timestamp_file):
                try:
                    os.remove(timestamp_file)
                    print(f"Removed empty timestamp file: {timestamp_file}")
                except Exception as e:
                    print(f"Warning: Failed to remove empty timestamp file - {e}")

    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error: Exception occurred during testing - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
