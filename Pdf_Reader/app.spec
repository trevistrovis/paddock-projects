# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(['server.py'],
             pathex=[],
             binaries=[],
             datas=[('flask_app/templates', 'flask_app/templates'), 
                    ('flask_app/static', 'flask_app/static'),
                    ('flask_app/uploads', 'flask_app/uploads'),
                    ('flask_app/controllers', 'flask_app/controllers'),
                    ('flask_app/models', 'flask_app/models')],
             hiddenimports=['flask', 'werkzeug', 'jinja2', 'sqlalchemy', 
                           'flask_sqlalchemy', 'flask_bcrypt', 'flask_login',
                           'sqlite3', 'fitz', 'pdfminer',
                           'pdfminer.high_level'],
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)

pyz = PYZ(a.pure, a.zipped_data,
          cipher=block_cipher)

exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='PaddockPDFReader',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=False,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None)