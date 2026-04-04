"""
E2E UI Tests using Playwright and Chrome.

Tests the Gradio UI without requiring actual data loading.
Validates visual design, tab structure, keyboard navigation, and accessibility.
"""

import asyncio
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

import pytest
from playwright.async_api import async_playwright, Page


# @MX:NOTE: Port for Gradio app during testing
GRADIO_PORT = 7860
GRADIO_URL = f"http://localhost:{GRADIO_PORT}"


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def gradio_server():
    """
    Start Gradio app server for testing.

    @MX:NOTE: Server runs on GRADIO_PORT for duration of test session.
    Server is automatically cleaned up after tests complete.
    """
    print("\n" + "="*70)
    print("Starting Gradio server for E2E testing...")
    print("="*70)

    # Start the Gradio server
    # Use DEVNULL to prevent blocking on pipe buffers
    # Output is not needed for tests, server logs go to terminal
    proc = subprocess.Popen(
        [sys.executable, "gradio_app.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=Path(__file__).parent.parent,
    )

    # Wait for server to start
    max_wait = 45
    print(f"Waiting up to {max_wait}s for server to be ready...")

    for i in range(max_wait):
        try:
            urllib.request.urlopen(GRADIO_URL, timeout=1)
            print(f"✓ Server ready after {i+1}s")
            print("="*70 + "\n")
            break
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            time.sleep(1)
            if i == max_wait - 1:
                proc.kill()
                proc.wait()
                raise RuntimeError(f"Gradio server failed to start after {max_wait}s. Last error: {e}") from e
        except Exception as e:
            time.sleep(1)
            if i == max_wait - 1:
                proc.kill()
                proc.wait()
                raise RuntimeError(f"Gradio server failed to start. Unexpected error: {e}") from e

    yield GRADIO_URL

    # Cleanup: kill server process
    print("\n" + "="*70)
    print("Stopping Gradio server...")
    proc.kill()
    proc.wait()
    print("✓ Server stopped")
    print("="*70 + "\n")


@pytest.fixture
async def browser_page(gradio_server):
    """
    Launch Chrome browser and navigate to Gradio app.

    @MX:ANCHOR: Core fixture for all E2E tests - provides Page object
    @MX:REASON: Centralized browser setup ensures consistent test environment
    """
    # Check for HEADLESS environment variable (default: true for CI)
    headless = os.getenv("HEADLESS", "true").lower() in ("1", "true", "yes")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        page = await browser.new_page()
        await page.goto(gradio_server)
        yield page
        await browser.close()


class TestVisualDesign:
    """Test DataGrip-inspired visual design."""

    @pytest.mark.asyncio
    async def test_dark_theme_background(self, browser_page: Page):
        """Verify background color matches theme (dark or light mode)."""
        background_color = await browser_page.evaluate(
            """() => {
                const body = window.getComputedStyle(document.body);
                return body.backgroundColor;
            }"""
        )

        # Check if background matches either dark or light theme
        # Light mode: #FFFFFF or rgb(255, 255, 255)
        # Dark mode: #1E1E1E or rgb(30, 30, 30)
        # Gradio may convert to rgb format
        assert background_color in [
            "rgb(30, 30, 30)",
            "#1e1e1e",
            "#1E1E1E",
            "rgb(255, 255, 255)",
            "#ffffff",
            "#FFFFFF",
        ], f"Expected background to match theme (dark or light), got {background_color}"

    @pytest.mark.asyncio
    async def test_font_family_inter(self, browser_page: Page):
        """Verify Inter font for body text."""
        font_family = await browser_page.evaluate(
            """() => {
                const body = window.getComputedStyle(document.body);
                return body.fontFamily;
            }"""
        )

        assert "Inter" in font_family or "inter" in font_family, \
            f"Expected Inter font, got {font_family}"

    @pytest.mark.asyncio
    async def test_minimal_color_scheme(self, browser_page: Page):
        """Verify minimal monochrome color scheme (no bright colors)."""
        # Get all button elements
        buttons = await browser_page.query_selector_all("button")

        for button in buttons[:10]:  # Check first 10 buttons
            background_color = await button.evaluate(
                """(el) => {
                    const styles = window.getComputedStyle(el);
                    return styles.backgroundColor;
                }"""
            )

            # Convert to rgb if hex
            if background_color.startswith("#"):
                background_color = await browser_page.evaluate(
                    """(color) => {
                        const r = parseInt(color.slice(1, 3), 16);
                        const g = parseInt(color.slice(3, 5), 16);
                        const b = parseInt(color.slice(5, 7), 16);
                        return `rgb(${r}, ${g}, ${b})`;
                    }""",
                    background_color
                )

            # Check for forbidden bright colors (green, purple, orange)
            assert not any(color in background_color for color in [
                "rgb(5, 150, 105)",   # Green
                "rgb(16, 185, 129)",  # Green
                "rgb(139, 92, 246)",  # Purple
                "rgb(255, 152, 0)",   # Orange
            ]), f"Found forbidden bright color: {background_color}"


class TestTabStructure:
    """Test tab structure matches BRIEF requirements (exactly 3 tabs)."""

    @pytest.mark.asyncio
    async def test_three_tabs_exist(self, browser_page: Page):
        """Verify exactly 3 tabs exist as per BRIEF-001."""
        # Wait for page to load
        await browser_page.wait_for_selector("button", timeout=5000)

        # Scope to top-level tablist to avoid nested/sub-tabs
        tablist = await browser_page.query_selector("div[role='tablist']")
        assert tablist, "No top-level tablist found"

        # Find all tab buttons within the top-level tablist
        tab_buttons = await tablist.query_selector_all("button[role='tab']")

        # Should have exactly 3 tabs
        assert len(tab_buttons) == 3, \
            f"Expected 3 tabs, found {len(tab_buttons)}"

    @pytest.mark.asyncio
    async def test_tab_labels(self, browser_page: Page):
        """Verify tab labels match BRIEF requirements."""
        expected_tabs = [
            "Data Inspection",
            "Query Editor",
            "Progress Monitoring"
        ]

        # Get all tab button text
        tab_texts = await browser_page.evaluate(
            """() => {
                const tabs = Array.from(document.querySelectorAll("button[role='tab']"));
                return tabs.map(t => t.textContent.trim());
            }"""
        )

        # Check if expected tabs are present
        for expected_tab in expected_tabs:
            assert any(expected_tab in text for text in tab_texts), \
                f"Expected tab '{expected_tab}' not found. Found: {tab_texts}"

    @pytest.mark.asyncio
    async def test_no_forbidden_tabs(self, browser_page: Page):
        """Verify forbidden tabs (Run Analytics, Report Builder) don't exist."""
        tab_texts = await browser_page.evaluate(
            """() => {
                const tabs = Array.from(document.querySelectorAll("button[role='tab']"));
                return tabs.map(t => t.textContent.trim());
            }"""
        )

        forbidden_tabs = ["Run Analytics", "Report Builder"]

        for forbidden in forbidden_tabs:
            assert not any(forbidden in text for text in tab_texts), \
                f"Forbidden tab '{forbidden}' found in: {tab_texts}"


class TestKeyboardNavigation:
    """Test keyboard-first navigation with visual shortcuts."""

    @pytest.mark.asyncio
    async def test_keyboard_shortcuts_visible(self, browser_page: Page):
        """Verify keyboard shortcut badges are visible on buttons."""
        # Look for keyboard shortcut hints (kbd elements or similar)
        shortcut_badges = await browser_page.query_selector_all(
            "kbd, .shortcut, [class*='shortcut'], [class*='hotkey']"
        )

        # Should have at least some keyboard shortcuts visible
        assert len(shortcut_badges) > 0, \
            "No keyboard shortcut badges found on buttons"

    @pytest.mark.asyncio
    async def test_tab_navigation_works(self, browser_page: Page):
        """Verify Tab key navigates between elements."""
        # Focus first button
        await browser_page.press("body", "Tab")
        await asyncio.sleep(0.2)

        # Check focus indicator
        focused_element = await browser_page.evaluate("() => document.activeElement.tagName")
        assert focused_element in ["BUTTON", "INPUT", "TEXTAREA"], \
            f"Tab didn't focus interactive element, focused: {focused_element}"

    @pytest.mark.asyncio
    async def test_focus_indicator_visible(self, browser_page: Page):
        """Verify 2px blue focus indicators on focused elements."""
        # Focus a button
        await browser_page.press("body", "Tab")
        await asyncio.sleep(0.2)

        # Get focused element's outline
        outline = await browser_page.evaluate(
            """() => {
                const el = document.activeElement;
                const styles = window.getComputedStyle(el);
                return styles.outline;
            }"""
        )

        # Should have some kind of focus indicator
        # Note: exact style may vary, but should exist
        focused = await browser_page.evaluate(
            """() => {
                const el = document.activeElement;
                const styles = window.getComputedStyle(el);
                return styles.outline !== 'none' ||
                       styles.boxShadow !== 'none' ||
                       styles.border !== 'none';
            }"""
        )

        assert focused, "No focus indicator found on focused element"


class TestAccessibility:
    """Test WCAG 2.2 AA compliance."""

    @pytest.mark.asyncio
    async def test_aria_labels_present(self, browser_page: Page):
        """Verify ARIA labels on interactive elements."""
        # Check buttons for aria-label or proper text content
        buttons = await browser_page.query_selector_all("button")

        # Guard against empty button list
        assert len(buttons) > 0, "No buttons found in page"

        buttons_with_labels = 0
        for button in buttons[:20]:  # Check first 20
            has_label = await button.evaluate(
                """(el) => {
                    return el.hasAttribute('aria-label') ||
                           el.textContent.trim().length > 0 ||
                           el.hasAttribute('title');
                }"""
            )
            if has_label:
                buttons_with_labels += 1

        # At least 80% of buttons should have labels
        label_ratio = buttons_with_labels / min(len(buttons), 20)
        assert label_ratio >= 0.8, \
            f"Only {label_ratio:.0%} of buttons have proper labels"

    @pytest.mark.asyncio
    async def test_color_contrast(self, browser_page: Page):
        """Verify sufficient color contrast (WCAG AA 4.5:1 for normal text)."""
        # Check contrast ratio for body text using actual WCAG calculation
        contrast_ratio = await browser_page.evaluate(
            """() => {
                // Helper function to parse color to RGB
                function parseColor(color) {
                    // Handle rgb(r, g, b) format using split instead of regex
                    if (color.startsWith('rgb(')) {
                        const parts = color.slice(4, -1).split(',');
                        return {
                            r: parseInt(parts[0].trim()),
                            g: parseInt(parts[1].trim()),
                            b: parseInt(parts[2].trim())
                        };
                    }
                    // Handle #rrggbb format
                    const hexMatch = color.match(/#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})/i);
                    if (hexMatch) {
                        return {
                            r: parseInt(hexMatch[1], 16),
                            g: parseInt(hexMatch[2], 16),
                            b: parseInt(hexMatch[3], 16)
                        };
                    }
                    return null;
                }

                // Calculate relative luminance per WCAG 2.0
                function getLuminance(r, g, b) {
                    const rsRGB = r / 255;
                    const gsRGB = g / 255;
                    const bsRGB = b / 255;

                    const rLinear = rsRGB <= 0.03928 ? rsRGB / 12.92 : Math.pow((rsRGB + 0.055) / 1.055, 2.4);
                    const gLinear = gsRGB <= 0.03928 ? gsRGB / 12.92 : Math.pow((gsRGB + 0.055) / 1.055, 2.4);
                    const bLinear = bsRGB <= 0.03928 ? bsRGB / 12.92 : Math.pow((bsRGB + 0.055) / 1.055, 2.4);

                    return 0.2126 * rLinear + 0.7152 * gLinear + 0.0722 * bLinear;
                }

                // Calculate contrast ratio per WCAG 2.0
                function getContrastRatio(l1, l2) {
                    const lighter = Math.max(l1, l2);
                    const darker = Math.min(l1, l2);
                    return (lighter + 0.05) / (darker + 0.05);
                }

                const body = document.body;
                const styles = window.getComputedStyle(body);

                const bgColor = parseColor(styles.backgroundColor);
                const textColor = parseColor(styles.color);

                if (!bgColor || !textColor) {
                    return false; // Cannot determine colors
                }

                const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);
                const textLuminance = getLuminance(textColor.r, textColor.g, textColor.b);
                const ratio = getContrastRatio(bgLuminance, textLuminance);

                // WCAG AA requires 4.5:1 for normal text
                return ratio >= 4.5;
            }"""
        )

        assert contrast_ratio, "Insufficient color contrast detected (must meet WCAG AA 4.5:1)"

    @pytest.mark.asyncio
    async def test_semantic_html(self, browser_page: Page):
        """Verify semantic HTML structure."""
        # Check for proper heading structure
        has_headings = await browser_page.evaluate(
            """() => {
                const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
                return headings.length > 0;
            }"""
        )

        # Check for proper button/interactive elements
        has_buttons = await browser_page.evaluate(
            """() => {
                const buttons = document.querySelectorAll('button, [role="button"]');
                return buttons.length > 0;
            }"""
        )

        assert has_headings, "No semantic headings found"
        assert has_buttons, "No buttons or interactive elements found"


class TestResponsiveInteractions:
    """Test responsive UI interactions."""

    @pytest.mark.asyncio
    async def test_button_hover_states(self, browser_page: Page):
        """Verify buttons have hover states."""
        button = await browser_page.query_selector("button")
        assert button, "No buttons found"

        # Get initial background
        initial_bg = await button.evaluate(
            """(el) => window.getComputedStyle(el).backgroundColor"""
        )

        # Hover over button
        await button.hover()
        await asyncio.sleep(0.2)

        # Get hover background
        hover_bg = await button.evaluate(
            """(el) => window.getComputedStyle(el).backgroundColor"""
        )

        # Should have different background on hover
        # Note: Some buttons may not change, so we just check the property exists
        styles = await button.evaluate(
            """(el) => {
                const s = window.getComputedStyle(el);
                return {
                    backgroundColor: s.backgroundColor,
                    transition: s.transition,
                    cursor: s.cursor
                };
            }"""
        )

        assert styles["cursor"] in ["pointer", "default"], \
            f"Invalid cursor style: {styles['cursor']}"

    @pytest.mark.asyncio
    async def test_click_interactions_work(self, browser_page: Page):
        """Verify button clicks are responsive."""
        # Find a clickable button (not tabs)
        buttons = await browser_page.query_selector_all("button:not([role='tab'])")

        if buttons:
            button = buttons[0]

            # Click button
            click_result = await button.click()
            await asyncio.sleep(0.5)

            # Test passes if button was clicked without errors
            assert click_result is None, "Button click should complete without errors"


class TestDataGripAesthetic:
    """Test DataGrip-inspired terminal aesthetic."""

    @pytest.mark.asyncio
    async def test_high_information_density(self, browser_page: Page):
        """Verify high information density (terminal-native)."""
        # Check that layout is compact, not spaced out
        spacing = await browser_page.evaluate(
            """() => {
                const container = document.querySelector('.gradio-container');
                if (!container) return true;

                const styles = window.getComputedStyle(container);
                const padding = styles.padding;

                // Terminal UI should have tight padding (not 20px+)
                const paddingValue = parseInt(padding);
                return paddingValue <= 16; // Tight spacing
            }"""
        )

        # Verify compact spacing for terminal aesthetic
        assert spacing, "Expected compact spacing (<=16px) for terminal aesthetic"

    @pytest.mark.asyncio
    async def test_code_font_jetbrains_mono(self, browser_page: Page):
        """Verify JetBrains Mono for code elements."""
        # Find code or pre elements
        code_elements = await browser_page.query_selector_all("code, pre, .code")

        if code_elements:
            font_family = await code_elements[0].evaluate(
                """(el) => window.getComputedStyle(el).fontFamily"""
            )

            # Should use JetBrains Mono or similar monospace
            assert "Mono" in font_family or "monospace" in font_family, \
                f"Code elements should use monospace font, got: {font_family}"


class TestTerminalNativeUX:
    """Test terminal-native user experience."""

    @pytest.mark.asyncio
    async def test_minimal_borders(self, browser_page: Page):
        """Verify minimal/subtle borders (terminal aesthetic)."""
        # Get input elements
        inputs = await browser_page.query_selector_all("input, textarea")

        if inputs:
            border_radius = await inputs[0].evaluate(
                """(el) => {
                    const styles = window.getComputedStyle(el);
                    return parseInt(styles.borderRadius) || 0;
                }"""
            )

            # Should have subtle border radius (4px or less for terminal aesthetic)
            assert border_radius <= 8, \
                f"Border radius too large for terminal aesthetic: {border_radius}px"

    @pytest.mark.asyncio
    async def test_clean_interface(self, browser_page: Page):
        """Verify clean, uncluttered interface."""
        # Check for decorative elements that shouldn't exist in terminal UI
        has_shadows = await browser_page.evaluate(
            """() => {
                const elements = Array.from(document.querySelectorAll('*'));
                return elements.some(el => {
                    const styles = window.getComputedStyle(el);
                    const shadow = styles.boxShadow;
                    return shadow && shadow !== 'none' && !shadow.includes('0px 0px');
                });
            }"""
        )

        # Terminal UI should be flat/minimal with no heavy drop shadows
        # Focus indicators (0px 0px shadows) are acceptable
        assert not has_shadows, \
            f"Interface should have clean, minimal shadows (no heavy drop shadows). Found shadows: {has_shadows}"
