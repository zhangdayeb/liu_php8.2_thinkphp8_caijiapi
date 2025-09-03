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
        $startTime = microtime(true);
        $output->writeln('开始执行用户在线状态检测...');
        
        try {
            // 记录开始日志
            Log::info('UserZaiXianState - 开始执行用户在线状态检测');
            
            // 统计数据
            $totalUsers = 0;
            $onlineUsers = 0;
            $offlineUsers = 0;
            $processedUsers = 0;
            
            // 获取5分钟前的时间
            $fiveMinutesAgo = date('Y-m-d H:i:s', strtotime('-5 minutes'));
            
            // 分批处理参数
            $batchSize = 500; // 每批处理500个用户
            $page = 1;
            
            while (true) {
                // 计算偏移量
                $offset = ($page - 1) * $batchSize;
                
                // 分页获取用户
                $users = Db::table('ntp_common_user')
                    ->field('id, name, state')
                    ->limit($offset, $batchSize)
                    ->select();
                
                // 如果没有更多用户，退出循环
                if (empty($users) || count($users) == 0) {
                    break;
                }
                
                $totalUsers += count($users);
                $output->writeln("正在处理第 {$page} 批用户，本批共 " . count($users) . " 个用户...");
                
                // 批量收集用户ID
                $userIds = array_column($users->toArray(), 'id');
                
                // 批量查询5分钟内有游戏活动的用户ID
                $activeGameUserIds = Db::table('ntp_game_user_money_logs')
                    ->where('member_id', 'in', $userIds)
                    ->where('created_at', '>=', $fiveMinutesAgo)
                    ->group('member_id')
                    ->column('member_id');
                
                // 批量查询5分钟内有登录记录的用户ID
                $activeLoginUserIds = Db::table('ntp_common_login_log')
                    ->where('unique', 'in', $userIds)
                    ->where('login_type', 2)  // 2表示用户登录
                    ->where('login_time', '>=', $fiveMinutesAgo)
                    ->group('unique')
                    ->column('unique');
                
                // 合并两个活跃用户ID数组（去重）
                $activeUserIds = array_unique(array_merge($activeGameUserIds, $activeLoginUserIds));
                
                // 准备批量更新数据
                $onlineUpdateIds = [];
                $offlineUpdateIds = [];
                
                foreach ($users as $user) {
                    if (in_array($user['id'], $activeUserIds)) {
                        // 用户有活动（游戏或登录），标记为在线
                        $onlineUpdateIds[] = $user['id'];
                        $onlineUsers++;
                        
                // 输出详细信息（可选，减少输出以提高性能）
                // if ($user['state'] != 1) {
                //     $output->info("用户 {$user['name']} (ID: {$user['id']}) 状态变更为在线");
                // }
                    } else {
                        // 用户无活动（无游戏且无登录），标记为离线
                        $offlineUpdateIds[] = $user['id'];
                        $offlineUsers++;
                        
                // 输出详细信息（可选，减少输出以提高性能）
                // if ($user['state'] != 0) {
                //     $output->comment("用户 {$user['name']} (ID: {$user['id']}) 状态变更为离线");
                // }
                    }
                    $processedUsers++;
                }
                
                // 批量更新在线状态
                if (!empty($onlineUpdateIds)) {
                    Db::table('ntp_common_user')
                        ->where('id', 'in', $onlineUpdateIds)
                        ->update([
                            'state' => 1,
                            'last_activity_at' => date('Y-m-d H:i:s')
                        ]);
                    
                    Log::debug('UserZaiXianState - 批量更新在线用户', [
                        'count' => count($onlineUpdateIds),
                        'user_ids' => array_slice($onlineUpdateIds, 0, 10) // 只记录前10个ID
                    ]);
                }
                
                // 批量更新离线状态
                if (!empty($offlineUpdateIds)) {
                    Db::table('ntp_common_user')
                        ->where('id', 'in', $offlineUpdateIds)
                        ->update(['state' => 0]);
                    
                    Log::debug('UserZaiXianState - 批量更新离线用户', [
                        'count' => count($offlineUpdateIds),
                        'user_ids' => array_slice($offlineUpdateIds, 0, 10) // 只记录前10个ID
                    ]);
                }
                
                // 进度输出
                $output->writeln("第 {$page} 批处理完成，已处理用户数: {$processedUsers}");
                
                $page++;
                
                // 防止内存溢出，释放变量
                unset($users, $userIds, $activeUserIds, $onlineUpdateIds, $offlineUpdateIds);
            }
            
            // 计算执行时间
            $endTime = microtime(true);
            $executionTime = round($endTime - $startTime, 2);
            
            // 输出统计结果
            $output->writeln('');
            $output->writeln('========== 执行完成 ==========');
            $output->writeln("总用户数: {$totalUsers}");
            $output->writeln("<info>在线用户数: {$onlineUsers}</info>");
            $output->writeln("<comment>离线用户数: {$offlineUsers}</comment>");
            $output->writeln("执行时间: {$executionTime} 秒");
            $output->writeln('==============================');
            
            // 记录执行日志
            Log::info('UserZaiXianState - 用户在线状态检测完成', [
                'total_users' => $totalUsers,
                'online_users' => $onlineUsers,
                'offline_users' => $offlineUsers,
                'execution_time' => $executionTime,
                'time_window' => '5分钟',
                'detection_sources' => ['game_money_logs', 'login_log']
            ]);

        } catch (\Exception $e) {
            $errorMsg = '用户在线状态检测失败: ' . $e->getMessage();
            $output->writeln("<error>{$errorMsg}</error>");
            
            Log::error('UserZaiXianState - 执行失败', [
                'error' => $e->getMessage(),
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'trace' => $e->getTraceAsString()
            ]);
            
            return 1; // 返回错误状态码
        }
        
        return 0; // 返回成功状态码
    }
    

}