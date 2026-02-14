#!/usr/bin/env python3
"""
Test if NYC Open Data works with requests.Session()
"""

import requests

# Create a session
session = requests.Session()

# Set headers to mimic a browser
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://data.cityofnewyork.us/',
})

print("Testing NYC Open Data with requests.Session()...\n")

# Test 1: Try to access the main page first (to get cookies)
print("1. Accessing main dataset page to establish session...")
main_url = 'https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc'
response = session.get(main_url)
print(f"   Status: {response.status_code}")
print(f"   Cookies: {len(session.cookies)} cookies received")
for cookie in session.cookies:
    print(f"     - {cookie.name}")

# Test 2: Try the old geospatial API endpoint
print("\n2. Trying old geospatial API endpoint...")
geo_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON'
response = session.get(geo_url)
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    print(f"   ✅ SUCCESS! Size: {len(response.content)} bytes")
else:
    print(f"   ❌ Failed: {response.text[:200]}")

# Test 3: Try Socrata API v2 endpoint
print("\n3. Trying Socrata API v2 endpoint...")
api_url = 'https://data.cityofnewyork.us/resource/d3c5-ddgc.geojson?$limit=5000'
response = session.get(api_url)
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    print(f"   ✅ SUCCESS! Size: {len(response.content)} bytes")
else:
    print(f"   ❌ Failed: {response.text[:200]}")

# Test 4: Try rows/export endpoint
print("\n4. Trying rows export endpoint...")
export_url = 'https://data.cityofnewyork.us/api/views/d3c5-ddgc/rows.json?accessType=DOWNLOAD'
response = session.get(export_url)
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    print(f"   ✅ SUCCESS! Size: {len(response.content)} bytes")
else:
    print(f"   ❌ Failed: {response.text[:200]}")

# Test 5: Try direct download link (if visible in page)
print("\n5. Trying to extract download link from page HTML...")
if 'download' in response.text.lower():
    print("   Found 'download' in page - may need to parse HTML for actual link")
else:
    print("   No obvious download link in response")

# Test 6: Check for any API info in page
print("\n6. Checking for API endpoints in page...")
import re
api_patterns = [
    r'api/views/[^"\']+',
    r'resource/[^"\']+',
    r'geospatial/[^"\']+',
]
response = session.get(main_url)
for pattern in api_patterns:
    matches = re.findall(pattern, response.text)
    if matches:
        print(f"   Found pattern '{pattern}': {matches[:3]}")

print("\n" + "="*70)
print("Session Test Complete")
