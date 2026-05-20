import os
import winreg

hadoop_home = r'C:\hadoop'
bin_path = os.path.join(hadoop_home, 'bin')

try:
    access = winreg.KEY_SET_VALUE | winreg.KEY_QUERY_VALUE | winreg.KEY_WOW64_64KEY
    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, 'Environment', 0, access) as env_key:
        winreg.SetValueEx(env_key, 'HADOOP_HOME', 0, winreg.REG_EXPAND_SZ, hadoop_home)
        try:
            current_path, _ = winreg.QueryValueEx(env_key, 'PATH')
        except FileNotFoundError:
            current_path = os.environ.get('PATH', '')
        if bin_path not in current_path:
            new_path = current_path + ';' + bin_path
            winreg.SetValueEx(env_key, 'PATH', 0, winreg.REG_EXPAND_SZ, new_path)
        print('HADOOP_HOME set to', hadoop_home)
        print('User PATH updated')
except PermissionError as e:
    print('PermissionError:', e)
    print('Try running this script as an administrator or set environment variables manually.')
