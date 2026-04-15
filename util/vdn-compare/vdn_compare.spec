# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['vdn_compare.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['duckdb', 'pandas', 'openpyxl', 'rich', 'rich.console', 'rich.table', 'rich.box', 'tqdm', 'tkinter', 'tabulate'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='vdn_compare',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
