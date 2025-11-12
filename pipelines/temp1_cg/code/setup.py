from setuptools import setup, find_packages
setup(
    name = 'temp1_cg',
    version = '1.0',
    packages = find_packages(include = ('temp1_cg*', )) + ['prophecy_config_instances.temp1_cg'],
    package_dir = {'prophecy_config_instances.temp1_cg' : 'configs/resources/temp1_cg'},
    package_data = {'prophecy_config_instances.temp1_cg' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.5'],
    entry_points = {
'console_scripts' : [
'main = temp1_cg.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
