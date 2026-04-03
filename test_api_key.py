#!/usr/bin/env python3
"""Test script to verify API key flow"""

import os
import sys

# Add current dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Test get_effective_gemini_api_key
from app_demo import get_effective_gemini_api_key

print("=== Testing API Key Resolution ===")

# Test 1: No key
result = get_effective_gemini_api_key(None)
print(f"Test 1 (no key): {bool(result)} - {result[:10] if result else 'None'}")

# Test 2: With override
result = get_effective_gemini_api_key("AIzaTest123")
print(f"Test 2 (override): {bool(result)} - {result}")

# Test 3: Check env var
env_key = os.environ.get("GEMINI_API_KEY")
print(f"Test 3 (env GEMINI_API_KEY): {bool(env_key)} - {env_key[:10] if env_key else 'Not set'}")

env_demo_key = os.environ.get("GEMINI_API_KEY_DEMO")
print(f"Test 4 (env GEMINI_API_KEY_DEMO): {bool(env_demo_key)} - {env_demo_key[:10] if env_demo_key else 'Not set'}")

# Test 4: Fallback
if env_key or env_demo_key:
    result = get_effective_gemini_api_key(None)
    print(f"Test 5 (fallback to env): {bool(result)}")
else:
    print("Test 5: No env keys set - must use UI-entered key")

print("\n=== Summary ===")
print("If no keys are available, the API will fail with 'getaddrinfo failed'")
print("because the google.genai SDK tries to use ADC and connects to")
print("a metadata server that doesn't exist.")
print("\nFix: Either set GEMINI_API_KEY env var or enter key in UI")
