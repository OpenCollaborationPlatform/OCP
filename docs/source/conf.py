# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'Open Collaboration Platform'
copyright = '2021, Stefan Tröger'
author = 'Stefan Tröger'

release = '0.1'
version = '0.1.0'

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
]

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'
