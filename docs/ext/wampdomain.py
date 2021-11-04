
import re

from dataclasses import dataclass
from docutils import nodes
from sphinx import addnodes
from sphinx.roles import XRefRole
from sphinx.domains import Domain
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
        
        signode += addnodes.desc_annotation("Procedure", "Procedure ")
        
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
        
        signode += addnodes.desc_annotation("Uri", "Uri ")
        
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
        
        signode += addnodes.desc_annotation("Event", "Event ")
        
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

    indices = []

    def clear_doc(self, docname):
        self.data['uris'] = []


    def get_objects(self):
        for obj in self.data['uris']:
            yield(obj)
            
    def resolve_xref(self, env, fromdocname, builder, typ,
                     target, node, contnode):
        
        print("resolve xref: ")
        
        matches = [(docname, anchor)
                 for name, dispname, otype, docname, anchor, prio
                 in self.get_objects() if dispname.contains(target) and otype == typ]
        
        if not matches:
            return None
        
        match = matches[0]
        return make_refnode(builder,fromdocname, docname,
                                anchor, contnode, anchor)


def setup(app):
    app.add_domain(WampDomain)
    return {"parallel_read_safe": False,
            "parallel_write_safe": False}
