# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'OCP Node'
copyright = '2021, Stefan Tröger'
author = 'Stefan Tröger'

release = '0.1'
version = '0.1.0'

# -- General configuration
import sys, os
sys.path.append(os.path.abspath("./../ext"))

extensions = [
    'sphinx.ext.duration',
    'wampdomain',
    'dmldomain',
]

autoapi_type = 'go'
autoapi_dirs = ['..']

templates_path = ['_templates']

# -- Options for HTML output

#html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'
