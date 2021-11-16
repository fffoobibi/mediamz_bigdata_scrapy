# 该模块负责生成vpn连接脚本

import platform
import subprocess
import tempfile

from os import mkdir
from os.path import exists
from .globals import g

__all__ = ('connect_vpn', )

vpn_script = '''
    $vpnName="%s";
    $user="%s";
    $paswd = "%s";
    $vpn = Get-VpnConnection -Name $vpnName
    if($vpn.ConnectionStatus -eq "Disconnected"){
    # rasdial $vpnName;
    rasdial $vpnName $user $paswd;
    };

    # set-executionpolicy remotesigner 开启执行权限
    ''' % (g.vpn_settings['name'], g.vpn_settings['user'], g.vpn_settings['passwd'])
 
def generate_script():
    if not exists('./scripts'):
        mkdir('./scripts')

    with open('./scripts/connect.ps1', 'w', encoding='utf8') as file:
        file.write(vpn_script)



def connect_vpn(spider):
    if g.vpn_settings['enable']:
        try:
            if platform.system() == 'Windows':
                p = subprocess.Popen(
                    'ping -n 2 -w 1000 www.tiktok.com', stdout=subprocess.PIPE)
                res = p.stdout.read().decode('gbk')
                if res.find('100% 丢失'):
                    spider.logger.info('连接vpn...')
                    p = subprocess.Popen('powershell.exe -File .\scripts\connect.ps1')
                    code = p.wait()
            else:
                pass
        except:
            pass