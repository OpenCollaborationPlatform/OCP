import builtins
import inspect
import re
import sys
import typing
import warnings
from inspect import Parameter
from typing import Any, Dict, Iterable, Iterator, List, NamedTuple, Optional, Tuple, Type, cast

from docutils import nodes
from docutils.nodes import Element, Node
from docutils.parsers.rst import directives
from docutils.parsers.rst.states import Inliner

from sphinx import addnodes
from sphinx.addnodes import desc_signature, pending_xref, pending_xref_condition
from sphinx.application import Sphinx
from sphinx.builders import Builder
from sphinx.deprecation import RemovedInSphinx50Warning
from sphinx.directives import ObjectDescription
from sphinx.domains import Domain, Index, IndexEntry, ObjType
from sphinx.environment import BuildEnvironment
from sphinx.locale import _, __
from sphinx.pycode.ast import ast
from sphinx.pycode.ast import parse as ast_parse
from sphinx.roles import XRefRole
from sphinx.util import logging
from sphinx.util.docfields import Field, GroupedField, TypedField
from sphinx.util.docutils import SphinxDirective
from sphinx.util.inspect import signature_from_str
from sphinx.util.nodes import find_pending_xref_condition, make_id, make_refnode
from sphinx.util.typing import OptionSpec, TextlikeNode

logger = logging.getLogger(__name__)



def create_xref(reftype: str, text: str, env: BuildEnvironment = None) -> addnodes.pending_xref:

    if env:
        kwargs = {'dml:class': env.ref_context.get('dml:class')}
    else:
        kwargs = {}

    contnodes = [nodes.Text(text)]
    return pending_xref('', *contnodes,
                        refdomain='dml', reftype=reftype, reftarget=text, **kwargs)


class ObjectEntry(NamedTuple):
    docname: str
    node_id: str
    objtype: str

class DmlObject(ObjectDescription[Tuple[str, str]]):
    """
    Description of a general Python object.

    :cvar allow_nesting: Class is an object that allows for nested namespaces
    :vartype allow_nesting: bool
    """
    option_spec: OptionSpec = {}

    allow_nesting = False

    def split_signature(self, sig: str) -> Tuple[str, bool, str, List[str], str]:
        """ Returns the signature information from the directive text, returns tuple
            - type (class, property, event, function) -> str
            - const (only valid for property and function) -> str
            - name -> str
            - arglist (only valid for function) -> List[str]
            - default (default value for property) -> str
        """
        pass

    def handle_signature(self, sig: str, signode: desc_signature) -> Tuple[str, str]:
        """Transform a DML signature into RST nodes.

        Return (fully qualified name of the thing, classname if any).
        """
 
        #relevant info: 
        (typ, name, arglist, annotations) = self.split_signature(sig)

        # determine class name as well as full name
        classname = self.env.ref_context.get('dml:class')
        if classname:
            # class name is not given in the signature
            fullname = classname + '.' + name
        else:
            # It is a class itself, so fullname = name
            classname = ""
            fullname = name
 
        signode['class'] = classname
        signode['fullname'] = fullname
          
        signode += addnodes.desc_annotation(str(typ), str(typ))
        signode += addnodes.desc_sig_space()
        signode += addnodes.desc_name(name, name)
        
        if arglist is not None:
                        
            # diff between None (no arguments) and [] (empty argument list, but brackets will be drawn)
            paramlist = addnodes.desc_parameterlist()
            for arg in arglist:
                
                if isinstance(arg, addnodes.pending_xref):
                    print("add pending xref")
                    paramlist += addnodes.desc_parameter('', '', arg)
                else:
                    paramlist += addnodes.desc_parameter(arg, arg)

            signode += paramlist
        
        if annotations:
            signode += addnodes.desc_sig_space()
            signode += addnodes.desc_sig_operator('', '[')
            addsepertor = False
            for anno in annotations:
                if addsepertor:
                    signode += addnodes.desc_sig_operator('', ',')
                    signode += addnodes.desc_sig_space()
                    
                signode += addnodes.desc_annotation(anno, anno)
                aaddseperator = True
                
            signode += addnodes.desc_sig_operator('', ']')
                           
        return fullname, classname


    def get_index_text(self, name: Tuple[str, str]) -> str:
        """Return the text for the index entry of the object."""
        raise NotImplementedError('must be implemented in subclasses')


    def add_target_and_index(self, name_cls: Tuple[str, str], sig: str,
                             signode: desc_signature) -> None:
        fullname = name_cls[0]
        node_id = make_id(self.env, self.state.document, '', fullname)
        signode['ids'].append(node_id)

        # Assign old styled node_id(fullname) not to break old hyperlinks (if possible)
        # Note: Will removed in Sphinx-5.0  (RemovedInSphinx50Warning)
        if node_id != fullname and fullname not in self.state.document.ids:
            signode['ids'].append(fullname)

        self.state.document.note_explicit_target(signode)

        domain = cast(DmlDomain, self.env.get_domain('dml'))
        domain.note_object(fullname, self.objtype, node_id, location=signode)


    def before_content(self) -> None:
        """Handle object nesting before content

        :py:class:`PyObject` represents Python language constructs. For
        constructs that are nestable, such as a Python classes, this method will
        build up a stack of the nesting hierarchy so that it can be later
        de-nested correctly, in :py:meth:`after_content`.

        For constructs that aren't nestable, the stack is bypassed, and instead
        only the most recent object is tracked. This object prefix name will be
        removed with :py:meth:`after_content`.
        """
        prefix = None
        if self.names:
            # fullname and name_prefix come from the `handle_signature` method.
            # fullname represents the full object name that is constructed using
            # object nesting and explicit prefixes. `name_prefix` is the
            # explicit prefix given in a signature
            (fullname, name_prefix) = self.names[-1]
            if self.allow_nesting:
                prefix = fullname
            elif name_prefix:
                prefix = name_prefix.strip('.')
        if prefix:
            self.env.ref_context['dml:class'] = prefix
            if self.allow_nesting:
                classes = self.env.ref_context.setdefault('dml:classes', [])
                classes.append(prefix)
 
    def after_content(self) -> None:
        """Handle object de-nesting after content

        If this class is a nestable object, removing the last nested class prefix
        ends further nesting in the object.

        If this class is not a nestable object, the list of classes should not
        be altered as we didn't affect the nesting levels in
        :py:meth:`before_content`.
        """
        classes = self.env.ref_context.setdefault('dml:classes', [])
        if self.allow_nesting:
            try:
                classes.pop()
            except IndexError:
                pass
        self.env.ref_context['dml:class'] = (classes[-1] if len(classes) > 0
                                            else None)

class DmlType(DmlObject):
    """
    Description of a type in the dml language
    """

    option_spec: OptionSpec = DmlObject.option_spec.copy()

    def split_signature(self, sig: str) -> Tuple[str, bool, str, List[str], str]:
        
        # return tuple:
        #   - type (class, property, event, function) -> str
        #   - name -> str
        #   - arglist -> List[str]
        #   - annotations -> List[str]
        
        typ = "type"
        name  =  sig
        arglist = None
        annotations = []
         
        return (typ, name, arglist, annotations)

    def get_index_text(self, modname: str, name_cls: Tuple[str, str]) -> str:
        return name_cls[0] + " (Type)"


class DmlFunction(DmlObject):
    """Description of a function."""

    option_spec: OptionSpec = DmlObject.option_spec.copy()
    option_spec.update({
        'const': directives.flag,
        'virtual': directives.flag,
    })
    
    doc_field_types = [
        TypedField('parameter', label=_('Parameters'),
                     names=('param', 'parameter', 'arg', 'argument',
                            'keyword', 'kwarg', 'kwparam'),
                     typenames=('paramtype', 'type'),
                     can_collapse=True),
        TypedField('result', label='Return',
                   names=('return', 'result'),
                   typenames=('returntype', 'resulttype')),
    ]

    def split_signature(self, sig: str) -> Tuple[str, bool, str, List[str], str]:
        
        # return tuple:
        #   - type (class, property, event, function) -> str
        #   - name -> str
        #   - arglist -> List[str]
        #   - annotations -> List[str]
        
        typ = "function"
                
        parts = sig.split("(")
        name = parts[0]
        
        parts = parts[1].split(")")
        arglist = parts[0].split(",")
        
        annotations = []
        if 'const' in self.options:
            annotations.append("constant")
        if 'virtual' in self.options:
            annotations.append("virtual")
            
        return (typ, name, arglist, annotations)

    def get_index_text(self, name_cls: Tuple[str, str]) -> str:
        # add index in own add_target_and_index() instead.
        return "FunctionName " + name_cls[0]


class DmlProperty(DmlObject):
    """Description of a function."""

    option_spec: OptionSpec = DmlObject.option_spec.copy()
    option_spec.update({
        'const': directives.flag,
        'readonly': directives.flag,
        'type': directives.unchanged,
    })
    
    doc_field_types = [
        Field('default', label=_('Default'), names=('default')),
        GroupedField('throws', label=_('Throws'), names=('throws', 'throw', 'error', 'err'), can_collapse=True)
    ]

    def split_signature(self, sig: str) -> Tuple[str, str, List[str], List[str]]:
        
        # return tuple:
        #   - type (class, property, event, function) -> str
        #   - name -> str
        #   - arglist -> List[str]
        #   - annotations -> List[str]
           
        if not "type" in self.options:
            raise Exception("DML property always needs type assigned")
        
        typ = "property " + self.options["type"]
        name = sig
        arglist = None

        annotations = []
        if 'const' in self.options:
            annotations.append("constant")
        if 'readonly' in self.options:
            annotations.append("readonly")   
            
        return (typ, name, arglist, annotations)


    def get_index_text(self, name_cls: Tuple[str, str]) -> str:
        # add index in own add_target_and_index() instead.
        return "PropertyName " + name_cls[0]
    
    
class DmlEvent(DmlObject):
    """Description of an event."""

    option_spec: OptionSpec = DmlObject.option_spec.copy()
    option_spec.update({
        'abstract': directives.flag,
    })
    
    doc_field_types = [
        TypedField('arguments', label=_('Arguments'), names=('arg', 'argument'), typenames=('argtype', 'argumenttype'))
    ]

    def split_signature(self, sig: str) -> Tuple[str, str, List[str], List[str]]:
        
        # return tuple:
        #   - type (class, property, event, function) -> str
        #   - name -> str
        #   - arglist -> List[str]
        #   - annotations -> List[str]
        
        typ = "event"
        name = sig
        arglist = None

        annotations = []
        if 'abstract' in self.options:
            annotations.append("abstract") 
            
        return (typ, name, arglist, annotations)


    def get_index_text(self, name_cls: Tuple[str, str]) -> str:
        # add index in own add_target_and_index() instead.
        return "EventName " + name_cls[0]
    
    

class DmlObject(DmlObject):
    """
    Description of a class
    """

    option_spec: OptionSpec = DmlObject.option_spec.copy()
    option_spec.update({
        'derived': directives.unchanged,
        'abstract': directives.flag,
    })
    allow_nesting = True

    def split_signature(self, sig: str) -> Tuple[str, bool, str, List[str], str]:
        
        # return tuple:
        #   - type (class, property, event, function) -> str
        #   - name -> str
        #   - arglist -> List[str]
        #   - annotations -> List[str]
        
        typ = "object"
        name  =  sig
        arglist = None
        if "derived" in self.options:
            arglist = [create_xref("obj", self.options["derived"], self.env)]
            
        annotations = []
        if 'abstract' in self.options:
            annotations.append("abstract")
        
        return (typ, name, arglist, annotations)

    def get_index_text(self, modname: str, name_cls: Tuple[str, str]) -> str:
        return name_cls[0] + " (Object)"


class DmlBehaviour(DmlObject):
    """
    Description of a behaviour
    """

    option_spec: OptionSpec = DmlObject.option_spec.copy()
    option_spec.update({
        'derived': directives.unchanged,
        'abstract': directives.flag,
    })
    allow_nesting = True

    def split_signature(self, sig: str) -> Tuple[str, bool, str, List[str], str]:
        
        # return tuple:
        #   - type (class, property, event, function) -> str
        #   - name -> str
        #   - arglist -> List[str]
        #   - annotations -> List[str]
        
        typ = "behaviour"
        name  =  sig
        arglist = None
        if "derived" in self.options:
            arglist = [create_xref("bhvr", self.options["derived"], self.env)]
            
        annotations = []
        if 'abstract' in self.options:
            annotations.append("abstract")
        
        return (typ, name, arglist, annotations)

    def get_index_text(self, modname: str, name_cls: Tuple[str, str]) -> str:
        return name_cls[0] + " (Behaviour)"

class DmlSystem(DmlObject):
    """
    Description of a class
    """

    option_spec: OptionSpec = DmlObject.option_spec.copy()
    allow_nesting = True

    def split_signature(self, sig: str) -> Tuple[str, bool, str, List[str], str]:
        
        # return tuple:
        #   - type (class, property, event, function) -> str
        #   - name -> str
        #   - arglist -> List[str]
        #   - annotations -> List[str]
        
        typ = "system"
        name  =  sig
        arglist = None       
        annotations = []
        
        return (typ, name, arglist, annotations)

    def get_index_text(self, modname: str, name_cls: Tuple[str, str]) -> str:
        return name_cls[0] + " (System)"


class DmlXRefRole(XRefRole):
    
    def process_link(self, env: BuildEnvironment, refnode: Element,
                     has_explicit_title: bool, title: str, target: str) -> Tuple[str, str]:
        #attach the py:class to the ref node, to make resolving easier

        refnode['dml:class'] = env.ref_context.get('dml:class')
        return title, target
     

class DmlDomain(Domain):
    """Python language domain."""
    name = 'dml'
    label = 'DML'
    object_types: Dict[str, ObjType] = {
        'function':     ObjType(_('function'),      'func',),
        'object':       ObjType(_('object'),        'obj'),
        'property':     ObjType(_('property'),      'prop'),
        'event':        ObjType(_('event'),         'evt'),
        'behaviour':    ObjType(_('behaviour'),     'bhvr'),
        'system':       ObjType(_('system'),        'sys'),
        'type':         ObjType(_('type'),          'type'),
   }

    directives = {
        'function': DmlFunction,
        'property': DmlProperty,
        'object':   DmlObject,
        'behaviour': DmlBehaviour,
        'event':    DmlEvent,
        'system':   DmlSystem,
        'type':     DmlType,
    }
    roles = {
        'func':  DmlXRefRole(),
        'obj':   DmlXRefRole(),
        'prop':  DmlXRefRole(),
        'evt': DmlXRefRole(),
        'bhvr': DmlXRefRole(),
        'sys':  DmlXRefRole(),
        'type': DmlXRefRole(),
    }
    
    initial_data: Dict[str, Dict[str, Tuple[Any]]] = {
        'objects': {},  # fullname -> docname, objtype
    }
    indices = [
        #PythonModuleIndex,
    ]

    @property
    def objects(self) -> Dict[str, ObjectEntry]:
        return self.data.setdefault('objects', {})  # fullname -> ObjectEntry

    def note_object(self, name: str, objtype: str, node_id: str, location: Any = None) -> None:

        if name in self.objects:
            # already registered
            return 

        self.objects[name] = ObjectEntry(self.env.docname, node_id, objtype)

 
    def clear_doc(self, docname: str) -> None:
        for fullname, obj in list(self.objects.items()):
            if obj.docname == docname:
                del self.objects[fullname]
  
    def merge_domaindata(self, docnames: List[str], otherdata: Dict) -> None:
        # XXX check duplicates?
        for fullname, obj in otherdata['objects'].items():
            if obj.docname in docnames:
                self.objects[fullname] = obj
 
    def find_obj(self, env: BuildEnvironment, classname: str,
                 name: str, type: str, searchmode: int = 0
                 ) -> List[Tuple[str, ObjectEntry]]:
        """Find a dml object for "name", perhaps using the classname.  
           Returns a list of (name, object entry) tuples.
        """
        # skip parens
        if name[-2:] == '()':
            name = name[:-2]

        if not name:
            return []

        matches: List[Tuple[str, ObjectEntry]] = []

        newname = None
        if searchmode == 1:
            if type is None:
                objtypes = list(self.object_types)
            else:
                objtypes = self.objtypes_for_role(type)
            if objtypes is not None:
                if classname:
                    fullname = classname + '.' + name
                    if fullname in self.objects and self.objects[fullname].objtype in objtypes:
                        newname = fullname
                if not newname:
                    if name in self.objects and self.objects[name].objtype in objtypes:
                        newname = name
                    else:
                        # "fuzzy" searching mode
                        searchname = '.' + name
                        matches = [(oname, self.objects[oname]) for oname in self.objects
                                   if oname.endswith(searchname) and
                                   self.objects[oname].objtype in objtypes]
        else:
            # NOTE: searching for exact match, object type is not considered
            if name in self.objects:
                newname = name
            elif classname and classname + '.' + name in self.objects:
                newname = classname + '.' + name
   
        if newname is not None:
            matches.append((newname, self.objects[newname]))
        return matches

    def resolve_xref(self, env: BuildEnvironment, fromdocname: str, builder: Builder,
                     type: str, target: str, node: pending_xref, contnode: Element
                     ) -> Optional[Element]:
        
        if "." in target:
            parts = target.split(".")
            clsname = parts[0]
            target = parts[1]
        else:
            clsname = node.get('dml:class')
        
        matches = self.find_obj(env, clsname, target, type, 1)

        if not matches:
            return None
        elif len(matches) > 1:
            logger.warning(__('more than one target found for cross-reference %r: %s'),
                               target, ', '.join(match[0] for match in matches),
                               type='ref', subtype='python', location=node)
        name, obj = matches[0]

        # determine the content of the reference by conditions
        content = find_pending_xref_condition(node, 'resolved')
        if content:
            children = content.children
        else:
            # if not found, use contnode
            children = [contnode]

        return make_refnode(builder, fromdocname, obj[0], obj[1], children, name)

    def get_objects(self) -> Iterator[Tuple[str, str, str, str, str, int]]:

        for refname, obj in self.objects.items():
            yield (refname, refname, obj.objtype, obj.docname, obj.node_id, 1)

    def get_full_qualified_name(self, node: Element) -> Optional[str]:
        clsname = node.get('dml:class')
        target = node.get('reftarget')
        if target is None:
            return None
        else:
            return '.'.join(filter(None, [clsname, target]))

def setup(app: Sphinx) -> Dict[str, Any]:
    
    app.add_domain(DmlDomain)

    return {
        'version': 'builtin',
        'env_version': 1,
        'parallel_read_safe': False,
        'parallel_write_safe': False
    }
 
