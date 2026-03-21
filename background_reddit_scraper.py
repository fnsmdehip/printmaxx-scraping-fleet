#!/usr/bin/env python3

from __future__ import annotations
"""
BACKGROUND REDDIT SCRAPER
=========================
Runs completely in the background - NO visible browser, NO interference.
Reddit doesn't require login for public subreddits, so this works immediately.

Usage:
    python3 background_reddit_scraper.py --scrape       # Top 20 subreddits
    python3 background_reddit_scraper.py --full         # All 40+ subreddits
    nohup python3 background_reddit_scraper.py --full > /tmp/reddit_scrape.log 2>&1 &
"""

import csv
import re
import json
import time
import argparse
import sys

csv.field_size_limit(sys.maxsize)
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)
from datetime import datetime

# Paths (auto-detected from script location)
PROJECT_DIR = Path(__file__).resolve().parent.parent
LEDGER_DIR = PROJECT_DIR / "LEDGER"
ALPHA_STAGING = LEDGER_DIR / "ALPHA_STAGING.csv"
RESEARCH_SUBREDDITS = LEDGER_DIR / "RESEARCH_SUBREDDITS.csv"
OUTPUT_DIR = PROJECT_DIR / "AUTOMATIONS" / "reddit_scraper_output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_subreddits(limit=None):
    """Load subreddits marked for auto-monitoring"""
    subreddits = []

    if RESEARCH_SUBREDDITS.exists():
        with open(RESEARCH_SUBREDDITS, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('auto_monitor') == 'TRUE':
                    subreddits.append({
                        'name': row.get('subreddit_name', '').replace('r/', ''),
                        'category': row.get('category', 'GENERAL'),
                        'members': row.get('member_count', '0')
                    })
    else:
        # Default high-value subreddits
        subreddits = [
            {'name': 'SaaS', 'category': 'SAAS', 'members': '341K'},
            {'name': 'Entrepreneur', 'category': 'BUSINESS', 'members': '3.2M'},
            {'name': 'SideProject', 'category': 'BUILDING', 'members': '150K'},
            {'name': 'indiehackers', 'category': 'BUILDING', 'members': '91K'},
            {'name': 'startups', 'category': 'STARTUP', 'members': '1.4M'},
            {'name': 'EntrepreneurRideAlong', 'category': 'BUSINESS', 'members': '200K'},
            {'name': 'juststart', 'category': 'SEO', 'members': '170K'},
            {'name': 'coldemail', 'category': 'OUTBOUND', 'members': '50K'},
            {'name': 'Affiliatemarketing', 'category': 'AFFILIATE', 'members': '180K'},
            {'name': 'passive_income', 'category': 'MONETIZATION', 'members': '750K'},
            {'name': 'MicroSaas', 'category': 'SAAS', 'members': '155K'},
            {'name': 'growthhacking', 'category': 'GROWTH', 'members': '200K'},
            {'name': 'ecommerce', 'category': 'ECOM', 'members': '300K'},
            {'name': 'dropship', 'category': 'ECOM', 'members': '100K'},
            {'name': 'Flipping', 'category': 'ECOM', 'members': '400K'},
            {'name': 'SEO', 'category': 'SEO', 'members': '500K'},
            {'name': 'bigseo', 'category': 'SEO', 'members': '100K'},
            {'name': 'AppBusiness', 'category': 'APPS', 'members': '50K'},
            {'name': 'iOSProgramming', 'category': 'APPS', 'members': '150K'},
            {'name': 'reactnative', 'category': 'APPS', 'members': '200K'},
        ]

    if limit:
        subreddits = subreddits[:limit]

    print(f"Loaded {len(subreddits)} subreddits to scrape")
    return subreddits


def get_next_alpha_id():
    """Get next available ALPHA ID"""
    max_id = 0
    if ALPHA_STAGING.exists():
        with open(ALPHA_STAGING, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                alpha_id = row.get('alpha_id', '')
                match = re.match(r'ALPHA(\d+)', alpha_id)
                if match:
                    max_id = max(max_id, int(match.group(1)))
    return max_id + 1


def load_existing_urls():
    """Load existing URLs to avoid duplicates"""
    urls = set()
    if ALPHA_STAGING.exists():
        with open(ALPHA_STAGING, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('source_url'):
                    urls.add(row['source_url'])
    return urls


def estimate_roi(text, upvotes=0):
    """Estimate ROI potential"""
    text_lower = text.lower()
    score = 0

    if re.search(r'\$[\d,]+k?', text_lower):
        score += 3
    if re.search(r'\d+%', text_lower):
        score += 2
    if 'mrr' in text_lower or 'arr' in text_lower:
        score += 2
    if upvotes > 100:
        score += 2
    elif upvotes > 50:
        score += 1

    if score >= 5:
        return 'HIGHEST'
    elif score >= 3:
        return 'HIGH'
    elif score >= 1:
        return 'MEDIUM'
    else:
        return 'LOW'


def has_signal(title):
    """Check if title has business/alpha signal"""
    title_lower = title.lower()
    patterns = [
        r'\$|\d+k|revenue|mrr|arr|\d+%|made|earn|profit|income|money',
        r'how\s*(i|to|do)|launch|built|ship|automat|tool|growth|scale',
        r'saas|app|startup|business|product|market|sell|customer|user',
        r'affiliate|newsletter|content|traffic|seo|ads|cold|email|outreach',
        r'ai|gpt|claude|llm|automation|no.?code|indie|solo|founder',
        r'tips|advice|help|question|feedback|review|strategy'
    ]
    for pattern in patterns:
        if re.search(pattern, title_lower):
            return True
    return False


def scrape_subreddits(subreddits):
    """Scrape subreddits using Reddit JSON API - runs in background"""
    existing_urls = load_existing_urls()
    next_id = get_next_alpha_id()
    new_entries = []

    print(f"\n{'='*60}")
    print(f"BACKGROUND REDDIT SCRAPER (JSON API)")
    print(f"{'='*60}")
    print(f"Subreddits to scrape: {len(subreddits)}")
    print(f"Starting ALPHA ID: ALPHA{next_id}")
    print(f"{'='*60}\n")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for i, sub in enumerate(subreddits):
        name = sub['name']
        try:
            print(f"[{i+1}/{len(subreddits)}] r/{name}...")

            # Use Reddit JSON API directly
            json_url = f"https://www.reddit.com/r/{name}/top.json?t=week&limit=25"
            resp = requests.get(json_url, headers=headers, timeout=30)

            posts = []
            if resp.status_code == 200:
                try:
                    data = resp.json()

                    for child in data.get('data', {}).get('children', []):
                        post_data = child.get('data', {})
                        title = post_data.get('title', '')
                        score = post_data.get('score', 0)
                        num_comments = post_data.get('num_comments', 0)
                        permalink = post_data.get('permalink', '')
                        url = f"https://www.reddit.com{permalink}" if permalink else post_data.get('url', '')

                        if title and len(title) > 15:
                            # Check for signal or high engagement
                            if has_signal(title) or score > 50:
                                posts.append({
                                    'title': title,
                                    'url': url,
                                    'score': score,
                                    'comments': num_comments
                                })

                    posts = posts[:10]  # Limit to top 10
                except json.JSONDecodeError:
                    posts = []

            for post in posts:
                if post['url'] not in existing_urls:
                    existing_urls.add(post['url'])

                    entry = {
                        'alpha_id': f'ALPHA{next_id}',
                        'source': f"r/{name}",
                        'source_url': post['url'],
                        'category': sub['category'],
                        'tactic': post['title'][:500],
                        'roi_potential': estimate_roi(post['title'], post['score']),
                        'priority': 'SOON',
                        'status': 'PENDING_REVIEW',
                        'applicable_methods': 'UNCHECKED',
                        'applicable_niches': 'N/A',
                        'engagement_authenticity': 'AUTHENTIC' if post['score'] > 10 else 'UNCHECKED',
                        'earnings_verified': 'N/A',
                        'created_at': datetime.now().isoformat(),
                        'notes': f"Score: {post['score']}, Comments: {post['comments']}"
                    }

                    new_entries.append(entry)
                    next_id += 1

            print(f"    → Found {len(posts)} posts with signal")
            time.sleep(1.5)  # Rate limiting

        except Exception as e:
            print(f"    ⚠️ Error: {str(e)[:50]}")
            continue

    # Save results
    if new_entries:
        fieldnames = ['alpha_id', 'source', 'source_url', 'category', 'tactic',
                      'roi_potential', 'priority', 'status', 'applicable_methods',
                      'applicable_niches', 'synergy_score', 'cross_sell_products',
                      'implementation_priority', 'engagement_authenticity',
                      'earnings_verified', 'extracted_method', 'compliance_notes',
                      'reviewer_notes', 'created_at', 'ops_generated',
                      'quality_issues', 'date_added']

        file_exists = ALPHA_STAGING.exists()
        with open(ALPHA_STAGING, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_entries)

        print(f"\n{'='*60}")
        print(f"✅ REDDIT SCRAPE COMPLETE")
        print(f"{'='*60}")
        print(f"New entries: {len(new_entries)}")
        print(f"Saved to: {ALPHA_STAGING}")

        json_path = OUTPUT_DIR / f"reddit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_path, 'w') as f:
            json.dump(new_entries, f, indent=2)
        print(f"JSON backup: {json_path}")

    return new_entries


def main():
    parser = argparse.ArgumentParser(description='Background Reddit Scraper')
    parser.add_argument('--scrape', action='store_true', help='Scrape top 20 subreddits')
    parser.add_argument('--full', action='store_true', help='Scrape ALL subreddits')
    parser.add_argument('--limit', type=int, help='Limit subreddits')

    args = parser.parse_args()

    if args.scrape or args.full:
        limit = args.limit or (20 if args.scrape else None)
        subreddits = load_subreddits(limit=limit)
        scrape_subreddits(subreddits)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
