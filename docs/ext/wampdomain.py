
import re

from dataclasses import dataclass
from docutils import nodes
from sphinx import addnodes
from sphinx.util.nodes import make_refnode
from sphinx.roles import XRefRole
from sphinx.domains import Domain, Index
from sphinx.directives import ObjectDescription
from sphinx.util.docfields import GroupedField, TypedField
from sphinx.locale import _

def getUri(sig):
    # remove parameters
    uri = sig.split("(")[0]
    # remove path indicators
    uri = uri.replace("<", "")
    uri = uri.replace(">", "")
    
    return uri

class Procedure(ObjectDescription):

    doc_field_types = [
        TypedField('parameter', label='Parameters',
                   names=('param', 'parameter', 'arg', 'argument'),
                   typenames=('paramtype', 'type')),
        TypedField('pathvar', label='Path',
                   names=('pathvar', 'pathvariable', 'path', 'var', 'varriable'),
                   typenames=('vartype', 'pathtype')),
        TypedField('result', label='Return',
                   names=('return', 'result'),
                   typenames=('returntype', 'resulttype')),
    ]

    option_spec = {}

    def handle_signature(self, sig, signode):
        
        signode += addnodes.desc_annotation("Procedure", "Procedure")
        signode += addnodes.desc_sig_space()
        
        # parse the path into nodes, including making <> things a parameter
        parts = sig.split('.')
        for part in parts[:-1]:
            if part.startswith("<"):
                part = part[1:-1]
                node = nodes.emphasis()
                node += addnodes.desc_addname(part, part+'.')
                signode += node
            else:
                signode += addnodes.desc_addname(part, part+'.')
        
        # parse procedure into name and parameters
        fnc = parts[-1].split("(")[0]
        signode += addnodes.desc_name(fnc, fnc)

        params = parts[-1].split("(")[1][:-1].split(",")
        paramlist = addnodes.desc_parameterlist()
        for param in params:
            paramlist += addnodes.desc_parameter(param, param)
        signode += paramlist
                       
        return sig
    
    def needs_arglist(self):
        return False

    def add_target_and_index(self, name_cls, sig, signode):
        signode['ids'].append(getUri(sig))
        objs = self.env.domaindata['wamp']['uris']
        
        anchor = getUri(sig)
        name = f"wamp.{type(self).__name__}.{anchor}"
        dispname = anchor
        otype = "proc"
        docname = self.env.docname
        prio = 0
        
        objs.append((name, dispname, otype, docname, anchor, prio))

    def get_index_text(self, modname, name):
        return ''
    
class Uri(ObjectDescription):

    doc_field_types = [
        TypedField('pathvar', label='Path',
                   names=('pathvar', 'pathvariable', 'path', 'var', 'varriable'),
                   typenames=('vartype', 'pathtype')),
    ]

    option_spec = {}

    def handle_signature(self, sig, signode):
        
        signode += addnodes.desc_annotation("Uri", "Uri")
        signode += addnodes.desc_sig_space()
        
        # parse the path into nodes, including making <> things a parameter
        parts = sig.split('.')
        for part in parts:
            if part.startswith("<"):
                part = part[1:-1]
                node = nodes.emphasis()
                node += addnodes.desc_addname(part, part+'.')
                signode += node
            else:
                signode += addnodes.desc_addname(part, part+'.')
                        
        return sig
    
    def needs_arglist(self):
        return False

    def add_target_and_index(self, name_cls, sig, signode):
        signode['ids'].append(getUri(sig))
        objs = self.env.domaindata['wamp']['uris']

        anchor = getUri(sig)
        name = f"wamp.{type(self).__name__}.{anchor}"
        dispname = anchor
        otype = "uri"
        docname = self.env.docname
        prio = 0
        
        objs.append((name, dispname, otype, docname, anchor, prio))
        
    def get_index_text(self, modname, name):
        return ''

class Event(ObjectDescription):

    doc_field_types = [
        TypedField('arguments', label='Arguments',
                   names=('param', 'parameter', 'arg', 'argument'),
                   typenames=('paramtype', 'type', 'argtype')),
        TypedField('pathvar', label='Path',
                   names=('pathvar', 'pathvariable', 'path', 'var', 'varriable'),
                   typenames=('vartype', 'pathtype')),
    ]

    option_spec = {}

    def handle_signature(self, sig, signode):
        
        signode += addnodes.desc_annotation("Event", "Event")
        signode += addnodes.desc_sig_space()
        
        # parse the path into nodes, including making <> things a parameter
        parts = sig.split('.')
        for part in parts[:-1]:
            if part.startswith("<"):
                part = part[1:-1]
                node = nodes.emphasis()
                node += addnodes.desc_addname(part, part+'.')
                signode += node
            else:
                signode += addnodes.desc_addname(part, part+'.')
        
        # parse procedure into name and parameters
        signode += addnodes.desc_name(parts[-1],parts[-1])     
        
        return sig
    
    def needs_arglist(self):
        return False

    def add_target_and_index(self, name_cls, sig, signode):
        signode['ids'].append(getUri(sig))
        objs = self.env.domaindata['wamp']['uris']

        anchor = getUri(sig)
        name = f"wamp.{type(self).__name__}.{anchor}"
        dispname = anchor
        otype = "evt"
        docname = self.env.docname
        prio = 0
        
        objs.append((name, dispname, otype, docname, anchor, prio))
        
    def get_index_text(self, modname, name):
        return ''

class WampIndex(Index):    
    name = 'index'
    localname = 'WAMP Index'
    shortname = 'WAMP'
    
    def __init__(self, *args, **kwargs):
        super(WampIndex, self).__init__(*args, **kwargs)

    def generate(self, docnames=None):
        """Return entries for the index given by *name*.  If *docnames* is
        given, restrict to entries referring to these docnames.
        The return value is a tuple of ``(content, collapse)``, where
        * collapse* is a boolean that determines if sub-entries should
        start collapsed (for output formats that support collapsing
        sub-entries).
        *content* is a sequence of ``(letter, entries)`` tuples, where *letter*
        is the "heading" for the given *entries*, usually the starting letter.
        *entries* is a sequence of single entries, where a single entry is a
        sequence ``[name, subtype, docname, anchor, extra, qualifier, descr]``.
        The items in this sequence have the following meaning:
        - `name` -- the name of the index entry to be displayed
        - `subtype` -- sub-entry related type:
          0 -- normal entry
          1 -- entry with sub-entries
          2 -- sub-entry
        - `docname` -- docname where the entry is located
        - `anchor` -- anchor for the entry within `docname`
        - `extra` -- extra info for the entry
        - `qualifier` -- qualifier for the description
        - `descr` -- description for the entry
        Qualifier and description are not rendered e.g. in LaTeX output.
        """

        content = {}
        
        procs = []
        evts = []
        uris = []
        for name, dispname, typ, docname, anchor, prio in self.domain.get_objects():
            if typ == "proc":
                procs.append((name, dispname, typ, docname, anchor))
            if typ == "evt":
                evts.append((name, dispname, typ, docname, anchor))
            if typ == "uri":
                uris.append((name, dispname, typ, docname, anchor))
                
        procs = sorted(procs, key=lambda item: item[1])
        evts = sorted(evts, key=lambda item: item[1])
        uris = sorted(uris, key=lambda item: item[1])
                 
        proc_entries = []
        for name, dispname, typ, docname, anchor in procs:
            proc_entries.append((
                dispname, 0, docname,
                anchor,
                docname, '', "Procedure"
            ))
            
        evt_entries = []
        for name, dispname, typ, docname, anchor in evts:
            evt_entries.append((
                dispname, 0, docname,
                anchor,
                docname, '', "Event"
            ))
            
        uri_entries = []
        for name, dispname, typ, docname, anchor in uris:
            uri_entries.append((
                dispname, 0, docname,
                anchor,
                docname, '', "Uri"
            ))
        
        content = [("Uri", uri_entries), ("Procedure", proc_entries), ("Events", evt_entries)]

        return (content, True)



class WampDomain(Domain):
    """Wamp domain."""

    name = 'wamp'
    label = 'WAMP'

    directives = {
        'procedure': Procedure,
        'event': Event,
        'uri': Uri,
    }

    roles = {
        'proc': XRefRole(),
        'evt': XRefRole(),
        'uri': XRefRole(),
    }

    initial_data = {
        'uris': [],
    }

    indices = {
        WampIndex,
    }

    def clear_doc(self, docname):
        self.data['uris'] = []


    def get_objects(self):
        for obj in self.data['uris']:
            yield(obj)
            
    def resolve_xref(self, env, fromdocname, builder, typ,
                     target, node, contnode):
        match = None
        for name, dispname, otype, docname, anchor, prio in self.get_objects():
            
            if otype == typ: 
                print(dispname,  target)
                if dispname.split(".")[-1] == target:
                    match = (docname, anchor)
                    break
        
        if not match:
            return None
        
        return make_refnode(builder,fromdocname, match[0],
                                match[1], contnode, match[1])


def setup(app):
    app.add_domain(WampDomain)
    return {"parallel_read_safe": False,
            "parallel_write_safe": False}
