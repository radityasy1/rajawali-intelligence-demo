#!/usr/bin/env python3
"""Merge BYOK features from index.html into index_test.html"""

import re

def main():
    # Read the source file (index_test.html)
    with open('templates/index_test.html', 'r', encoding='utf-8') as f:
        content = f.read()

    # ========== 1. ADD BYOK CSS STYLES ==========
    # Find the location after .dropdown-btn:hover styles
    byok_css = '''

        /* ======================================== */
        /*          API KEY MANAGEMENT STYLES       */
        /* ======================================== */
        .api-key-control {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 6px 10px;
            border: 1px solid var(--border-color);
            border-radius: 999px;
            background: var(--bg-card);
            color: var(--color-text);
            font-size: 0.82rem;
        }

        .api-key-status-pill {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            padding: 3px 8px;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 700;
            background: rgba(239, 68, 68, 0.12);
            color: #b91c1c;
        }

        .api-key-status-pill.loaded {
            background: rgba(34, 197, 94, 0.16);
            color: #166534;
        }

        .api-key-btn {
            border: 1px solid var(--border-color);
            background: transparent;
            color: var(--color-text);
            padding: 4px 10px;
            border-radius: 999px;
            cursor: pointer;
            font-size: 0.78rem;
            font-weight: 600;
        }

        .api-key-btn:hover {
            background: rgba(99, 102, 241, 0.08);
        }

        /* API Key Modal Styles */
        .api-key-modal-overlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.8);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 10001;
        }
        .api-key-modal {
            background: #1a1a2e;
            padding: 30px;
            border-radius: 15px;
            max-width: 500px;
            width: 90%;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
            color: #fff;
        }
        .api-key-modal h2 {
            color: #fff;
            margin-bottom: 10px;
        }
        .api-key-modal p {
            color: #aaa;
            margin-bottom: 20px;
        }
        .api-key-modal input[type="password"]:focus {
            outline: none;
            border-color: #4CAF50;
        }
        .api-key-modal button:hover {
            opacity: 0.9;
        }
'''

    # Insert after .dropdown-btn:hover styles
    pattern = r'(.dropdown-btn:hover \{[^}]+\})'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        content = content[:insert_pos] + byok_css + content[insert_pos:]
        print("[OK] Added BYOK CSS styles")
    else:
        print("[FAIL] Could not find insertion point for CSS")

    # ========== 2. ADD API KEY MODAL ==========
    modal_html = '''
    <!-- ======================================== -->
    <!--      0. API KEY MODAL (BYOK)             -->
    <!-- ======================================== -->
    <div class="api-key-modal-overlay" id="api-key-modal" style="display: none;">
        <div class="api-key-modal">
            <h2>🔑 Enter Your Gemini API Key</h2>
            <p>This demo requires a Google Gemini API key to function. Your key is stored locally and never sent to our servers.</p>

            <input type="password" id="api-key-input" placeholder="Enter your Gemini API Key..." style="
                width: 100%;
                padding: 15px;
                border: 1px solid #333;
                border-radius: 8px;
                background: #16213e;
                color: #fff;
                font-size: 14px;
                margin-bottom: 15px;
                box-sizing: border-box;
            ">

            <div id="api-key-status" style="
                margin-top: 10px;
                padding: 10px;
                border-radius: 5px;
                display: none;
            "></div>

            <label style="color: #aaa; font-size: 12px; display: block; margin-bottom: 15px;">
                <input type="checkbox" id="remember-key" checked>
                Remember for this browser
            </label>

            <div style="display: flex; gap: 10px;">
                <button onclick="useDemoKey()" style="
                    flex: 1;
                    padding: 12px 20px;
                    border: none;
                    border-radius: 8px;
                    cursor: pointer;
                    font-size: 14px;
                    font-weight: bold;
                    background: #333;
                    color: #aaa;
                ">Use Demo Key</button>
                <button onclick="submitApiKey()" style="
                    flex: 1;
                    padding: 12px 20px;
                    border: none;
                    border-radius: 8px;
                    cursor: pointer;
                    font-size: 14px;
                    font-weight: bold;
                    background: #4CAF50;
                    color: white;
                ">Continue</button>
            </div>

            <p style="margin-top: 15px; font-size: 12px;">
                <a href="https://makersuite.google.com/app/apikey" target="_blank" style="color: #4CAF50;">
                    Get your free API key →
                </a>
            </p>
        </div>
    </div>

'''

    # Insert before the main app div
    pattern = r'(<div class="app-container" id="main-app"[^>]*>)'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.start()
        content = content[:insert_pos] + modal_html + content[insert_pos:]
        print("[OK] Added API Key Modal HTML")
    else:
        print("[FAIL] Could not find insertion point for modal")

    # ========== 3. ADD API KEY CONTROLS TO HEADER ==========
    # Find the user-info span and add controls after it
    pattern = r'(<span id="user-info"[^>]*>[^<]*</span>)'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        api_controls = '''

                <div class="api-key-control">
                    <span id="api-key-status-pill" class="api-key-status-pill">No API Key</span>
                    <button id="manage-api-key-btn" class="api-key-btn" type="button">API Key</button>
                    <button id="clear-api-key-btn" class="api-key-btn" type="button" style="display: none;">Clear</button>
                </div>
'''
        content = content[:insert_pos] + api_controls + content[insert_pos:]
        print("[OK] Added API Key Controls to header")
    else:
        print("[FAIL] Could not find header to add controls")

    # ========== 4. ADD BUILD MARKER CONSOLE LOG ==========
    # Add after DOMContentLoaded event listener setup
    pattern = r'(document\.addEventListener\([\'"]DOMContentLoaded[\'"],\s*\(\)\s*=>\s*\{)'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        build_marker = '''
            // Build marker for debugging
            window.RAJAWALI_FRONTEND_BUILD = 'index_test BYOK-merged-v1';
            console.log('[Rajawali] Frontend build:', window.RAJAWALI_FRONTEND_BUILD);

            // Check and log API key status
            const storedKey = localStorage.getItem('rajawali_gemini_api_key');
            console.log('[Rajawali] API Key status:', storedKey ? 'Loaded (' + storedKey.slice(0, 4) + '...)' : 'Not set');
'''
        content = content[:insert_pos] + build_marker + content[insert_pos:]
        print("[OK] Added build marker and API key logging")
    else:
        print("[FAIL] Could not find DOMContentLoaded listener")

    # ========== 5. ADD API KEY DOM ELEMENT REFERENCES ==========
    # Find const userInfoSpan and add after
    pattern = r"(const userInfoSpan = document\.getElementById\('user-info'\);)"
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        dom_refs = '''
            const manageApiKeyBtn = document.getElementById('manage-api-key-btn');
            const clearApiKeyBtn = document.getElementById('clear-api-key-btn');
'''
        content = content[:insert_pos] + dom_refs + content[insert_pos:]
        print("[OK] Added API Key button DOM references")
    else:
        print("[FAIL] Could not find userInfoSpan declaration")

    # ========== 6. ADD EVENT LISTENERS FOR API KEY BUTTONS ==========
    # Find a good spot for event listeners - after the main menu dropdown setup
    pattern = r'(mainMenuDropdown\.querySelectorAll\([\'"]\.dropdown-item[\'"]\)\.forEach\(item\s*=>\s*\{[^}]+\}\);)'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        event_listeners = '''

            // API Key Management Event Listeners
            if (manageApiKeyBtn) {
                manageApiKeyBtn.addEventListener('click', () => {
                    const statusEl = document.getElementById('api-key-status');
                    if (statusEl) statusEl.style.display = 'none';
                    showApiKeyModal();
                });
            }

            if (clearApiKeyBtn) {
                clearApiKeyBtn.addEventListener('click', () => clearApiKey());
            }
'''
        content = content[:insert_pos] + event_listeners + content[insert_pos:]
        print("[OK] Added API Key event listeners")
    else:
        print("[FAIL] Could not find spot for event listeners")

    # ========== 7. ADD API KEY FUNCTIONS BEFORE lucide.createIcons() ==========
    api_key_js = '''
        // ========================================
        // API KEY MANAGEMENT (Portfolio Demo)
        // ========================================
        const API_KEY_STORAGE_KEY = 'rajawali_gemini_api_key';

        function getStoredGeminiApiKey() {
            return window.GEMINI_API_KEY || localStorage.getItem(API_KEY_STORAGE_KEY) || '';
        }

        function maskApiKey(key) {
            if (!key) return 'No API Key';
            if (key.length <= 8) return 'API Key Loaded';
            return `Key ${key.slice(0, 4)}...${key.slice(-4)}`;
        }

        function refreshApiKeyUI() {
            const pill = document.getElementById('api-key-status-pill');
            const clearBtn = document.getElementById('clear-api-key-btn');
            if (!pill || !clearBtn) return;

            const key = getStoredGeminiApiKey();
            if (key) {
                pill.textContent = maskApiKey(key);
                pill.classList.add('loaded');
                clearBtn.style.display = 'inline-flex';
            } else {
                pill.textContent = 'No API Key';
                pill.classList.remove('loaded');
                clearBtn.style.display = 'none';
            }
        }

        function buildRequestHeaders(sessionToken = null, includeJson = false) {
            const headers = {};
            if (includeJson) headers['Content-Type'] = 'application/json';
            if (sessionToken) headers['Authorization'] = `Bearer ${sessionToken}`;

            const geminiApiKey = getStoredGeminiApiKey();
            if (geminiApiKey) {
                headers['X-Gemini-API-Key'] = geminiApiKey;
            }

            return headers;
        }

        function checkApiKey() {
            const savedKey = localStorage.getItem(API_KEY_STORAGE_KEY);
            if (savedKey) {
                window.GEMINI_API_KEY = savedKey;
                refreshApiKeyUI();
                return true;
            }
            refreshApiKeyUI();
            return false;
        }

        function showApiKeyModal() {
            document.getElementById('api-key-modal').style.display = 'flex';
        }

        function hideApiKeyModal() {
            document.getElementById('api-key-modal').style.display = 'none';
        }

        function submitApiKey() {
            const key = document.getElementById('api-key-input').value.trim();
            const remember = document.getElementById('remember-key').checked;
            const statusEl = document.getElementById('api-key-status');

            if (!key) {
                statusEl.textContent = 'Please enter an API key';
                statusEl.style.background = '#ffebee';
                statusEl.style.color = '#c62828';
                statusEl.style.display = 'block';
                return;
            }

            // Validate key format (basic check)
            if (!key.startsWith('AIza')) {
                statusEl.textContent = 'Invalid API key format. Gemini keys start with "AIza"';
                statusEl.style.background = '#ffebee';
                statusEl.style.color = '#c62828';
                statusEl.style.display = 'block';
                return;
            }

            // Save key
            if (remember) {
                localStorage.setItem(API_KEY_STORAGE_KEY, key);
            }
            window.GEMINI_API_KEY = key;
            refreshApiKeyUI();

            statusEl.textContent = 'API key saved successfully!';
            statusEl.style.background = '#e8f5e9';
            statusEl.style.color = '#2e7d32';
            statusEl.style.display = 'block';

            setTimeout(() => {
                hideApiKeyModal();
            }, 500);
        }

        function useDemoKey() {
            // Demo key is fetched from backend
            fetch('/api/demo-key')
                .then(res => res.json())
                .then(data => {
                    if (data.key) {
                        document.getElementById('api-key-input').value = data.key;
                        submitApiKey();
                    } else {
                        alert('Demo key not available. Please use your own API key.');
                    }
                })
                .catch(() => {
                    alert('Demo key not available. Please use your own API key.');
                });
        }

        function clearApiKey() {
            localStorage.removeItem(API_KEY_STORAGE_KEY);
            window.GEMINI_API_KEY = null;
            refreshApiKeyUI();
            showApiKeyModal();
        }

        // Check API key on page load (after login)
        // This should be called after successful login
        function checkApiKeyAfterLogin() {
            if (!checkApiKey()) {
                showApiKeyModal();
            }
        }

'''

    # Find lucide.createIcons() and insert before it
    pattern = r'(\n        lucide\.createIcons\(\);)'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.start()
        content = content[:insert_pos] + api_key_js + content[insert_pos:]
        print("[OK] Added API Key JavaScript functions")
    else:
        print("[FAIL] Could not find lucide.createIcons()")

    # ========== 8. ADD refreshApiKeyUI() CALL IN showApp() ==========
    # Find showApp function and add refreshApiKeyUI() call
    pattern = r'(loginOverlay\.style\.display = [\'"]none[\'"];\s*mainApp\.style\.display = [\'"]flex[\'"];)'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        content = content[:insert_pos] + '\n                refreshApiKeyUI();' + content[insert_pos:]
        print("[OK] Added refreshApiKeyUI() call in showApp()")
    else:
        print("[FAIL] Could not find showApp function")

    # ========== 9. ADD checkApiKeyAfterLogin() CALL AFTER LOGIN ==========
    # Find the login success handler
    pattern = r"(showApp\(result\.user_info\);)"
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        content = content[:insert_pos] + '\n                            checkApiKeyAfterLogin();' + content[insert_pos:]
        print("[OK] Added checkApiKeyAfterLogin() call after login")
    else:
        print("[FAIL] Could not find showApp(result.user_info) call")

    # ========== 10. REPLACE buildRequestHeaders USAGE IN FETCH CALLS ==========
    # This is complex, let's find and update the key fetch calls

    # Save the modified content
    with open('templates/index_test.html', 'w', encoding='utf-8') as f:
        f.write(content)

    print("\n[OK] Merge complete! File saved to templates/index_test.html")
    print("\nNote: Some fetch calls may need manual review to use buildRequestHeaders()")

if __name__ == '__main__':
    main()
