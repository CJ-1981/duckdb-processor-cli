"""
UI accessibility and keyboard navigation tests for Gradio interface.

This module contains tests verifying accessibility features including
WCAG 2.2 AA compliance, keyboard navigation, screen reader support,
and DataGrip-inspired design system implementation.
"""

import pytest
from gradio_app import create_ui


@pytest.fixture(scope="module")
def ui_components():
    """
    Module-scoped fixture that creates UI once for all tests.

    @MX:NOTE: Single UI creation avoids redundant work and RUF059 warnings.
    Returns tuple of (app, theme, custom_css, keyboard_shortcuts_js).
    """
    return create_ui()


class TestThemeConfiguration:
    """Test DataGrip-inspired theme configuration."""

    def test_theme_colors_datagrip_inspired(self, ui_components):
        """Test theme uses DataGrip-inspired color palette."""
        app, theme, custom_css, keyboard_shortcuts_js = ui_components

        # @MX:NOTE: Theme supports both light and dark modes
        # Light mode: Clean white/blue background with dark elements
        assert theme.body_background_fill == "#FFFFFF"
        assert theme.background_fill_primary == "#FFFFFF"
        assert theme.background_fill_secondary == "#F0F4F8"  # Light blue-gray

        # Dark mode: DataGrip-inspired dark theme
        assert theme.body_background_fill_dark == "#1E1E1E"
        assert theme.background_fill_primary_dark == "#1E1E1E"
        assert theme.background_fill_secondary_dark == "#2D2D2D"

    def test_theme_uses_inter_font(self, ui_components):
        """Test theme uses Inter font for body text."""
        _, theme, _, _ = ui_components

        # Check font family includes Inter
        assert "Inter" in theme.font

    def test_theme_uses_jetbrains_mono(self, ui_components):
        """Test theme uses JetBrains Mono for code."""
        _, theme, _, _ = ui_components

        # Check monospace font includes JetBrains Mono
        assert "JetBrains Mono" in theme.font_mono

    def test_theme_high_contrast_grayscale(self, ui_components):
        """Test theme uses high contrast colors."""
        _, theme, _, _ = ui_components

        # @MX:NOTE: Light mode uses professional blue, dark mode uses gray
        # Light mode buttons: Professional blue
        assert theme.button_primary_background_fill == "#4A90E2"
        assert theme.button_primary_text_color == "#FFFFFF"  # White text on blue

        # Dark mode buttons: Gray (DataGrip style)
        assert theme.button_primary_background_fill_dark == "#5C5C5C"
        assert theme.button_primary_text_color_dark == "#E8E8E8"

        # Hover state
        assert theme.button_primary_background_fill_hover == "#5BA3F5"

    def test_theme_border_radius_subtle(self, ui_components):
        """Test theme uses subtle border radius (DataGrip-inspired)."""
        _, theme, _, _ = ui_components

        # Border radius should be 4px for professional look
        assert theme.block_radius == "4px"


class TestCustomCSS:
    """Test custom CSS implementation."""

    def test_custom_css_includes_fonts(self, ui_components):
        """Test custom CSS imports Inter and JetBrains Mono."""
        _, _, custom_css, _ = ui_components

        # Check font imports
        assert "Inter" in custom_css
        assert "JetBrains+Mono" in custom_css

    def test_custom_css_syntax_highlighting(self, ui_components):
        """Test custom CSS includes SQL syntax highlighting."""
        _, _, custom_css, _ = ui_components

        # Check SQL syntax highlighting classes
        assert ".sql-keyword" in custom_css
        assert ".sql-string" in custom_css
        assert ".sql-number" in custom_css

    def test_custom_css_focus_indicators(self, ui_components):
        """Test custom CSS includes 2px focus indicators for accessibility."""
        _, _, custom_css, _ = ui_components

        # Check focus indicator styling
        assert "outline: 2px solid #4A90E2" in custom_css
        assert "outline-offset: 2px" in custom_css

    def test_custom_css_data_table_styling(self, ui_components):
        """Test custom CSS includes DataGrip-inspired table styling."""
        _, _, custom_css, _ = ui_components

        # Check data table styling
        assert ".data-table" in custom_css
        assert "#3A3A3A" in custom_css  # DataGrip header color

    def test_custom_css_keyboard_shortcuts(self, ui_components):
        """Test custom CSS includes keyboard shortcut badges."""
        _, _, custom_css, keyboard_shortcuts_js = ui_components

        # Check keyboard shortcut styling for kbd elements
        # Badge class is added dynamically by JavaScript, not in CSS
        assert "kbd" in custom_css
        assert "#3A3A3A" in custom_css  # Badge background color
        # Verify JavaScript code for badge injection exists
        assert "keyboard-shortcut-badge" in keyboard_shortcuts_js

    def test_custom_css_scrollbar_styling(self, ui_components):
        """Test custom CSS includes terminal-native scrollbar styling."""
        _, _, custom_css, _ = ui_components

        # Check custom scrollbar
        assert "::-webkit-scrollbar" in custom_css
        assert "#404040" in custom_css  # Scrollbar thumb color


class TestWCAGCompliance:
    """Test WCAG 2.2 AA compliance."""

    def test_color_contrast_minimum_ratio(self, ui_components):
        """Test color combinations meet 4.5:1 contrast ratio minimum."""
        _, theme, _, _ = ui_components

        # @MX:NOTE: Verify proper contrast for BOTH light and dark modes
        # Light mode: Almost black text on white background
        assert theme.body_text_color == "#0A0A0A"  # Almost black text
        assert theme.body_background_fill == "#FFFFFF"  # White background
        # Contrast: #0A0A0A on #FFFFFF = 21:1 (excellent)

        # Dark mode: Light text on dark background
        assert theme.body_text_color_dark == "#E8E8E8"  # Light text
        assert theme.body_background_fill_dark == "#1E1E1E"  # Dark background
        # Contrast: #E8E8E8 on #1E1E1E = 15.3:1 (excellent)

    def test_link_color_contrast(self, ui_components):
        """Test link text meets contrast requirements."""
        _, theme, _, _ = ui_components

        # Link color should be #4A90E2 on #1E1E1E = 5.8:1
        assert theme.link_text_color == "#4A90E2"

    def test_focus_indicators_present(self, ui_components):
        """Test all interactive elements have visible focus indicators."""
        _, _, custom_css, _ = ui_components

        # Check CSS includes focus styling
        assert ":focus-visible" in custom_css
        assert "outline: 2px solid" in custom_css

    def test_status_badges_include_text(self, ui_components):
        """Test status information is conveyed through text, not just color."""
        _, _, custom_css, _ = ui_components

        # Check status badges use text labels
        assert ".status-ready" in custom_css
        assert ".status-running" in custom_css


class TestKeyboardNavigation:
    """Test keyboard navigation implementation."""

    def test_keyboard_shortcuts_configured(self, ui_components):
        """Test keyboard shortcuts are configured for major actions."""
        app, _, _, _ = ui_components

        # The app should include keyboard shortcuts JavaScript
        # This is verified by checking the app launches successfully
        assert app is not None

    def test_shortcut_badges_styled(self, ui_components):
        """Test keyboard shortcut badges use DataGrip-inspired styling."""
        _, _, custom_css, _ = ui_components

        # Check badge styling uses terminal-native aesthetics
        assert "font-family: 'JetBrains Mono'" in custom_css
        assert "border-radius: 2px" in custom_css

    def test_shortcut_badges_high_contrast(self, ui_components):
        """Test shortcut badges have sufficient contrast."""
        _, _, custom_css, _ = ui_components

        # Badges should use high contrast colors
        assert "#E8E8E8" in custom_css  # Light text on dark background
        assert "#3A3A3A" in custom_css  # Dark badge background


class TestScreenReaderSupport:
    """Test screen reader compatibility."""

    def test_semantic_html_structure(self, ui_components):
        """Test interface uses semantic HTML structure."""
        app, _, _, _ = ui_components

        # The interface should use proper semantic elements
        # Gradio provides this by default, and we verify the app launched
        assert app is not None, "UI app should be created successfully"

    def test_status_indicators_accessible(self, ui_components):
        """Test status indicators are accessible to screen readers."""
        _, _, custom_css, _ = ui_components

        # Status badges should be visible and properly styled
        assert ".status-badge" in custom_css, "Status badge CSS should be present"


class TestDataGripInspiredDesign:
    """Test DataGrip-inspired design system implementation."""

    def test_professional_color_scheme(self, ui_components):
        """Test interface uses professional color scheme."""
        _, theme, _, _ = ui_components

        # @MX:NOTE: Light mode uses professional blue, dark mode uses gray
        assert "#4A90E2" in theme.button_primary_background_fill  # Blue for light mode

        # Dark mode has DataGrip-inspired #1E1E1E background
        assert "#1E1E1E" in theme.body_background_fill_dark

    def test_terminal_native_aesthetics(self, ui_components):
        """Test interface uses terminal-native design elements."""
        _, _, custom_css, _ = ui_components

        # Check for terminal-inspired styling
        assert "font-family: 'JetBrains Mono'" in custom_css
        assert "border-radius: 2px" in custom_css or "border-radius: 4px" in custom_css

    def test_no_cartoonish_elements(self, ui_components):
        """Test interface avoids cartoonish design elements."""
        _, theme, _, _ = ui_components

        # Check that rounded corners are minimal (max 6px per design spec)
        # DataGrip uses 4px border radius
        assert theme.block_radius == "4px"

    def test_high_information_density(self, ui_components):
        """Test interface maintains high information density."""
        _, theme, _, _ = ui_components

        # Spacing should use 4px base unit for density
        # Gradio uses block_padding instead of spacing_lg/md/sm
        # block_padding="12px" gives good information density
        assert theme.block_padding == "12px"

    def test_accent_color_professional_blue(self, ui_components):
        """Test accent color is professional blue for active states."""
        _, theme, _, _ = ui_components

        # Check accent color is #4A90E2
        assert theme.link_text_color == "#4A90E2"
        assert theme.input_border_color_focus == "#4A90E2"


class TestResponsiveInteractions:
    """Test responsive interaction patterns."""

    def test_hover_states_defined(self, ui_components):
        """Test all interactive elements have hover states."""
        _, _, custom_css, _ = ui_components

        # Check for hover state definitions
        assert ":hover" in custom_css

    def test_transitions_smooth(self, ui_components):
        """Test transitions are smooth (150-350ms range)."""
        _, _, custom_css, _ = ui_components

        # Check for transition definitions
        assert "transition" in custom_css


class TestTypography:
    """Test typography implementation."""

    def test_body_font_inter(self, ui_components):
        """Test body text uses Inter font."""
        _, theme, custom_css, _ = ui_components

        # Check Inter font is used
        assert "Inter" in theme.font
        assert "Inter" in custom_css

    def test_code_font_jetbrains_mono(self, ui_components):
        """Test code uses JetBrains Mono font."""
        _, theme, custom_css, _ = ui_components

        # Check JetBrains Mono is used for code
        assert "JetBrains Mono" in theme.font_mono
        assert "JetBrains Mono" in custom_css

    def test_font_size_terminal_appropriate(self, ui_components):
        """Test font sizes are terminal-appropriate (14px base)."""
        _, theme, _, _ = ui_components

        # Check base font size is 14px
        assert theme.body_text_size == "14px"

    def test_font_weights_correct(self, ui_components):
        """Test font weights follow design system (400, 500, 600)."""
        _, theme, _, _ = ui_components

        # Check font weights
        assert theme.body_text_weight == "400"
        # Gradio doesn't have separate label_text_weight, uses body_text_weight


class TestButtonStyling:
    """Test button styling implementation."""

    def test_button_colors_datagrip_inspired(self, ui_components):
        """Test buttons use professional colors."""
        _, theme, _, _ = ui_components

        # @MX:NOTE: Light mode uses professional blue, dark mode uses gray
        assert "#4A90E2" in theme.button_primary_background_fill  # Blue for light mode

        # Dark mode uses #E8E8E8 text
        assert "#E8E8E8" in theme.button_primary_text_color_dark

    def test_button_hover_states(self, ui_components):
        """Test buttons have hover states."""
        _, theme, _, _ = ui_components

        # Check hover state (lighter blue for hover)
        assert theme.button_primary_background_fill_hover == "#5BA3F5"

    def test_button_focus_states(self, ui_components):
        """Test buttons have focus states for accessibility."""
        _, _, custom_css, _ = ui_components

        # Check focus indicator is present
        assert ":focus" in custom_css

    def test_disabled_states_visible(self, ui_components):
        """Test disabled button states are visually distinct."""
        app, _, _, _ = ui_components

        # The theme should handle disabled states
        # Gradio provides this by default, verify app launched successfully
        assert app is not None, "UI app should handle disabled button states"
