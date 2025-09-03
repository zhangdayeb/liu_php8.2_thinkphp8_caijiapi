<?php
declare (strict_types = 1);

namespace app\command;

use think\console\Command;
use think\console\Input;
use think\console\Output;
use think\facade\Db;
use think\facade\Log;

class UserZaiXianState extends Command
{
    protected function configure()
    {
        // 指令配置
        $this->setName('user_zaixian_state')
            ->setDescription('检测用户在线状态，根据游戏资金日志判断用户是否在线');
    }

    protected function execute(Input $input, Output $output)
    {

    }
    

}