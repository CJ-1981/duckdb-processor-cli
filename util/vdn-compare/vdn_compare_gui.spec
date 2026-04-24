# -*- mode: python ; coding: utf-8 -*-
#
# vdn_compare_gui.spec
#
# Build with:  python -m PyInstaller vdn_compare_gui.spec
#
# The --add-data entry bundles vdn_compare.py into the same temp directory
# that PyInstaller extracts to (sys._MEIPASS) at runtime.  The sys.path fix
# at the top of vdn_compare_gui.py adds that directory to sys.path so
# "import vdn_compare" works whether the exe is run headless (CLI) or
# double-clicked (GUI).

a = Analysis(
    ['vdn_compare_gui.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('vdn_compare.py', '.'),   # bundle CLI module alongside GUI entry point
    ],
    hiddenimports=[
        'vdn_compare',
        'duckdb',
        'pandas',
        'rich',
        'rich.console',
        'rich.table',
        'rich.box',
        'tqdm',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='vdn_compare_gui',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    # console=True  → always shows a console window.
    # This is intentional: CLI output (including Rich tables) is visible
    # when the exe is invoked from a terminal, and the window stays open
    # when the GUI is used (the user chose option 1).
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    onefile=True,
)
