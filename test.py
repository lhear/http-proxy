#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiohttp
import time
from typing import List, Tuple
import sys
import argparse
import ipaddress

# Test target URL
TEST_URL = "https://1.1.1.1"


def is_ip_in_cidr_list(ip_str: str, cidr_list: List[ipaddress.IPv4Network]) -> bool:
    """
    Check if the given IP string is within any of the provided CIDR networks.
    Returns True if it should be skipped.
    """
    try:
        ip = ipaddress.IPv4Address(ip_str)
        for cidr in cidr_list:
            if ip in cidr:
                return True
    except ipaddress.AddressValueError:
        # Not a valid IPv4 address (e.g., hostname). Do not skip.
        pass
    return False


def filter_proxies_by_cidr(proxies: List[str], skip_cidr_list: List[ipaddress.IPv4Network]) -> List[str]:
    """
    Remove proxies whose IP falls within any of the excluded CIDR ranges.
    Assumes proxy format is 'host:port'. Only filters if host is a valid IPv4.
    """
    if not skip_cidr_list:
        return proxies

    filtered = []
    for proxy in proxies:
        host = proxy.split(':')[0].strip()
        if not is_ip_in_cidr_list(host, skip_cidr_list):
            filtered.append(proxy)
        # else: skip this proxy
    return filtered


def read_cidr_list_from_file(filename: str) -> List[ipaddress.IPv4Network]:
    """
    Read CIDR ranges from a file (one per line, ignore empty lines and comments).
    """
    try:
        cidrs = []
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    try:
                        network = ipaddress.IPv4Network(line, strict=False)
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


async def test_http_proxy(proxy: str, session: aiohttp.ClientSession, timeout: int) -> Tuple[str, bool, float, str]:
    """
    Test the availability of a single HTTP proxy.
    """
    start_time = time.time()
    try:
        http_proxy_url = f"http://{proxy}"

        async with session.get(
            TEST_URL,
            proxy=http_proxy_url,
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as response:
            response_time = time.time() - start_time

            if response.status == 200:
                return proxy, True, response_time, ""
            else:
                return proxy, False, response_time, f"HTTP {response.status}"

    except asyncio.TimeoutError:
        response_time = time.time() - start_time
        return proxy, False, response_time, "Timeout"
    except Exception as e:
        response_time = time.time() - start_time
        return proxy, False, response_time, str(e)


async def test_all_proxies(proxies: List[str], timeout: int, max_concurrent: int) -> List[Tuple[str, bool, float, str]]:
    """
    Test all HTTP proxies concurrently.
    """
    if not proxies:
        return []
    connector = aiohttp.TCPConnector(limit=max_concurrent)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [test_http_proxy(proxy.strip(), session, timeout) for proxy in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append((proxies[i].strip(), False, 0.0, f"Exception: {str(result)}"))
            else:
                processed_results.append(result)

        return processed_results


def read_proxies_from_file(filename: str) -> List[str]:
    """Read proxy list from file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            proxies = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        return proxies
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Failed to read file - {e}")
        sys.exit(1)


def save_results_to_file(results: List[Tuple[str, bool, float, str]], filename: str):
    """Save available proxies to output file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for proxy, is_available, response_time, error in results:
                if is_available:
                    f.write(f"{proxy}\n")
        print(f"Available proxies saved to: {filename}")
    except Exception as e:
        print(f"Warning: Failed to save results file - {e}")


def print_results(results: List[Tuple[str, bool, float, str]], round_num: int = None):
    """Print test results."""
    available_count = sum(1 for r in results if r[1])
    total_count = len(results)

    header = f"\n{'='*80}"
    if round_num is not None:
        header += f"\nRound {round_num} Test Results"
    header += f"\nTest Results (Total: {total_count} proxies)"
    header += f"\n{'='*80}"
    print(header)

    for proxy, is_available, response_time, error in results:
        if is_available:
            time_str = f"{response_time:.2f}s"
            print(f"{proxy:<20} | {time_str}")

    print(f"{'='*80}")
    success_rate = (available_count / total_count * 100) if total_count > 0 else 0
    print(f"Total: {total_count} proxies, Available: {available_count}, Unavailable: {total_count - available_count}")
    print(f"Success Rate: {success_rate:.1f}%")


def main():
    parser = argparse.ArgumentParser(description='Test HTTP proxy availability (stop after 3 consecutive 100% passes)')
    parser.add_argument('input_file', help='Input file containing HTTP proxy list (host:port)')
    parser.add_argument('-o', '--output', help='Output file to save final available proxies')
    parser.add_argument('-t', '--timeout', type=int, default=5, help='Timeout in seconds (default: 5)')
    parser.add_argument('-c', '--concurrent', type=int, default=70, help='Max concurrent connections (default: 70)')
    parser.add_argument('--max-rounds', type=int, default=30, help='Max test rounds to prevent infinite loop (default: 30)')
    parser.add_argument('--skip-cidr', help='File containing CIDR ranges to skip (one per line)')

    args = parser.parse_args()

    # Read initial proxy list
    print(f"Reading proxy list from file '{args.input_file}'...")
    raw_proxies = read_proxies_from_file(args.input_file)

    if not raw_proxies:
        print("Error: Proxy list is empty")
        sys.exit(1)

    # Deduplicate proxy list
    unique_proxies = list(dict.fromkeys(raw_proxies))  # preserves order
    if len(unique_proxies) != len(raw_proxies):
        print(f"Deduplicated: {len(raw_proxies)} → {len(unique_proxies)} proxies")

    # Load and apply CIDR skip list if provided
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

    consecutive_success = 0  # Consecutive successful rounds
    round_num = 0

    try:
        while consecutive_success < 3 and round_num < args.max_rounds:
            round_num += 1
            print(f"\n>>> Starting Round {round_num} (Current consecutive success: {consecutive_success})")

            results = asyncio.run(test_all_proxies(current_proxies, args.timeout, args.concurrent))
            print_results(results, round_num)

            available_proxies = [proxy for proxy, ok, _, _ in results if ok]
            total = len(current_proxies)
            available = len(available_proxies)

            if total == 0:
                print("Error: Current proxy list is empty, terminating test")
                break

            if available == total:
                consecutive_success += 1
                print(f"Round {round_num} passed 100%! Consecutive success: {consecutive_success}/3")
            else:
                consecutive_success = 0  # Reset counter
                print(f"Round {round_num} did not pass 100%, resetting consecutive counter")
                current_proxies = available_proxies  # Next round tests only successful ones

            if not available_proxies:
                print("No proxies passed this round, terminating early")
                break

        # Loop ended
        if consecutive_success >= 3:
            print("\nPassed 3 consecutive rounds with 100% success! Testing complete.")
        else:
            print(f"\nDid not achieve 3 consecutive 100% passes (max rounds {args.max_rounds} reached or no proxies left)")

        # Save final results
        if args.output and current_proxies:
            print("Re-testing final proxy list for up-to-date status...")
            final_results = asyncio.run(test_all_proxies(current_proxies, args.timeout, args.concurrent))
            available_final = [r for r in final_results if r[1]]
            if available_final:
                save_results_to_file(available_final, args.output)
            else:
                print("Warning: No proxies available in final test, skipping output file")
        elif args.output:
            print("Warning: No proxies to save, skipping output file")

    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error: Exception occurred during testing - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
