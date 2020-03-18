import pytest
from traitlets import Float, TraitError

from .. import Tool
from ..tool import export_tool_config_to_commented_yaml


def test_tool_simple():
    """test the very basic functionality of a Tool"""

    class MyTool(Tool):
        description = "test"
        userparam = Float(5.0, help="parameter").tag(config=True)

    tool = MyTool()
    tool.userparam = 1.0
    tool.log_level = 'DEBUG'
    tool.log.info("test")
    with pytest.raises(SystemExit) as exc:
        tool.run([])
    assert exc.value.code == 0

    # test parameters changes:
    tool.userparam = 4.0
    with pytest.raises(TraitError):
        tool.userparam = "badvalue"


def test_tool_version():
    """ check that the tool gets an automatic version string"""
    class MyTool(Tool):
        description = "test"
        userparam = Float(5.0, help="parameter").tag(config=True)

    tool = MyTool()
    assert tool.version_string != ""


def test_export_config_to_yaml():
    """ test that we can export a Tool's config to YAML"""
    import yaml
    from ctapipe.tools.camdemo import CameraDemo

    tool = CameraDemo()
    tool.num_events = 2
    yaml_string = export_tool_config_to_commented_yaml(tool)

    # check round-trip back from yaml:
    config_dict = yaml.load(yaml_string, Loader=yaml.SafeLoader)

    assert config_dict['CameraDemo']['num_events'] == 2


def test_tool_html_rep():
    """ check that the HTML rep for Jupyter notebooks works"""
    class MyTool(Tool):
        description = "test"
        userparam = Float(5.0, help="parameter").tag(config=True)

    class MyTool2(Tool):
        """ A docstring description"""
        userparam = Float(5.0, help="parameter").tag(config=True)

    tool = MyTool()
    tool2 = MyTool2()
    assert len(tool._repr_html_()) > 0
    assert len(tool2._repr_html_()) > 0


def test_tool_current_config():
    """ Check that we can get the full instance configuration """
    class MyTool(Tool):
        description = "test"
        userparam = Float(5.0, help="parameter").tag(config=True)

    tool = MyTool()
    conf1 = tool.get_current_config()
    tool.userparam = -1.0
    conf2 = tool.get_current_config()

    assert conf1['MyTool']['userparam'] == 5.0
    assert conf2['MyTool']['userparam'] == -1.0


def test_tool_exit_code():
    """ Check that we can get the full instance configuration """
    from ctapipe.core.tool import run_tool

    class MyTool(Tool):

        description = "test"
        userparam = Float(5.0, help="parameter").tag(config=True)

    tool = MyTool()

    with pytest.raises(SystemExit) as exc:
        tool.run(['--non-existent-option'])

    assert exc.value.code == 1

    with pytest.raises(SystemExit) as exc:
        tool.run(['--MyTool.userparam=foo'])

    assert exc.value.code == 1

    assert run_tool(tool, ['--help']) == 0
    assert run_tool(tool, ['--non-existent-option']) == 1
