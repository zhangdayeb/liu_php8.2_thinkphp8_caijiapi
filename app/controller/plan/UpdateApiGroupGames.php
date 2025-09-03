<?php
namespace app\controller\plan;

use app\BaseController;
use think\facade\Db;
use think\facade\Log;

class UpdateApiGroupGames extends BaseController
{
    /**
     * 更新ntp_api_group_games表
     * 将ntp_group_set表中的游戏配置转换为以游戏为主体的集团关系配置
     */
    public function update_api_group_games()
    {
        try {
            Log::info('开始更新ntp_api_group_games表');
            $startTime = microtime(true);
            
            // 第一步：清空目标表
            $this->clearTargetTable();
            
            // 第二步：获取集团配置数据
            $groupConfigs = $this->getGroupConfigs();
            if (empty($groupConfigs)) {
                Log::warning('未找到集团配置数据');
                return $this->error('未找到集团配置数据');
            }
            
            // 第三步：收集游戏与集团的关系数据
            $gameRelations = $this->collectGameRelations($groupConfigs);
            if (empty($gameRelations)) {
                Log::warning('未收集到有效的游戏关系数据');
                return $this->error('未收集到有效的游戏关系数据');
            }
            
            // 第四步：验证游戏ID是否存在
            $validGameRelations = $this->validateGameIds($gameRelations);
            if (empty($validGameRelations)) {
                Log::warning('没有有效的游戏ID');
                return $this->error('没有有效的游戏ID');
            }
            
            // 第五步：批量插入数据
            $insertedCount = $this->batchInsertGameRelations($validGameRelations);
            
            $endTime = microtime(true);
            $duration = round(($endTime - $startTime) * 1000, 2);
            
            Log::info('ntp_api_group_games表更新完成', [
                'processed_groups' => count($groupConfigs),
                'inserted_games' => $insertedCount,
                'duration_ms' => $duration
            ]);
            
            return $this->success('游戏集团关系表更新成功');
            
        } catch (\Exception $e) {
            Log::error('更新ntp_api_group_games表失败', [
                'error' => $e->getMessage(),
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'trace' => $e->getTraceAsString()
            ]);
            
            return $this->error('更新失败: ' . $e->getMessage());
        }
    }
    
    /**
     * 清空目标表
     */
    private function clearTargetTable()
    {
        Log::info('开始清空ntp_api_group_games表');
        
        $deletedCount = Db::table('ntp_api_group_games')->delete(true);
        
        Log::info('清空ntp_api_group_games表完成', [
            'deleted_count' => $deletedCount
        ]);
    }
    
    /**
     * 获取集团配置数据
     */
    private function getGroupConfigs()
    {
        Log::info('开始获取集团配置数据');
        
        $configs = Db::table('ntp_group_set')
            ->field('group_prefix, game_show_ids, game_run_ids')
            ->where('status', 1) // 只获取启用状态的集团
            ->where('group_prefix', '<>', '') // group_prefix不为空
            ->whereNotNull('group_prefix')
            ->select()
            ->toArray();
        
        Log::info('获取集团配置数据完成', [
            'group_count' => count($configs)
        ]);
        
        return $configs;
    }
    
    /**
     * 收集游戏与集团的关系数据
     */
    private function collectGameRelations($groupConfigs)
    {
        Log::info('开始收集游戏关系数据');
        
        $gameShowMap = []; // 游戏ID => [可展示的集团前缀数组]
        $gameRunMap = [];  // 游戏ID => [可运行的集团前缀数组]
        
        foreach ($groupConfigs as $group) {
            $groupPrefix = trim($group['group_prefix']);
            
            if (empty($groupPrefix)) {
                continue;
            }
            
            // 处理展示权限
            if (!empty($group['game_show_ids'])) {
                $showGameIds = $this->parseGameIds($group['game_show_ids']);
                foreach ($showGameIds as $gameId) {
                    if (!isset($gameShowMap[$gameId])) {
                        $gameShowMap[$gameId] = [];
                    }
                    if (!in_array($groupPrefix, $gameShowMap[$gameId])) {
                        $gameShowMap[$gameId][] = $groupPrefix;
                    }
                }
                
                Log::debug('处理集团展示权限', [
                    'group_prefix' => $groupPrefix,
                    'show_game_count' => count($showGameIds)
                ]);
            }
            
            // 处理运行权限
            if (!empty($group['game_run_ids'])) {
                $runGameIds = $this->parseGameIds($group['game_run_ids']);
                foreach ($runGameIds as $gameId) {
                    if (!isset($gameRunMap[$gameId])) {
                        $gameRunMap[$gameId] = [];
                    }
                    if (!in_array($groupPrefix, $gameRunMap[$gameId])) {
                        $gameRunMap[$gameId][] = $groupPrefix;
                    }
                }
                
                Log::debug('处理集团运行权限', [
                    'group_prefix' => $groupPrefix,
                    'run_game_count' => count($runGameIds)
                ]);
            }
        }
        
        // 合并数据：所有涉及的游戏ID
        $allGameIds = array_unique(array_merge(array_keys($gameShowMap), array_keys($gameRunMap)));
        
        $gameRelations = [];
        foreach ($allGameIds as $gameId) {
            $showGroups = isset($gameShowMap[$gameId]) ? $gameShowMap[$gameId] : [];
            $runGroups = isset($gameRunMap[$gameId]) ? $gameRunMap[$gameId] : [];
            
            $gameRelations[$gameId] = [
                'game_id' => $gameId,
                'show_group_prefix' => implode(',', $showGroups),
                'run_group_prefix' => implode(',', $runGroups)
            ];
        }
        
        Log::info('收集游戏关系数据完成', [
            'total_game_relations' => count($gameRelations),
            'show_relations' => count($gameShowMap),
            'run_relations' => count($gameRunMap)
        ]);
        
        return $gameRelations;
    }
    
    /**
     * 解析游戏ID字符串
     */
    private function parseGameIds($gameIdsStr)
    {
        if (empty($gameIdsStr)) {
            return [];
        }
        
        // 分割字符串并过滤无效值
        $gameIds = array_filter(
            array_map('trim', explode(',', $gameIdsStr)),
            function($id) {
                return is_numeric($id) && $id > 0;
            }
        );
        
        // 转换为整数并去重
        return array_unique(array_map('intval', $gameIds));
    }
    
    /**
     * 验证游戏ID是否在ntp_api_games表中存在
     */
    private function validateGameIds($gameRelations)
    {
        Log::info('开始验证游戏ID有效性');
        
        $gameIds = array_keys($gameRelations);
        if (empty($gameIds)) {
            return [];
        }
        
        // 查询存在的游戏ID
        $existingGameIds = Db::table('ntp_api_games')
            ->where('id', 'in', $gameIds)
            ->column('id');
        
        // 过滤出有效的游戏关系
        $validGameRelations = [];
        $invalidGameIds = [];
        
        foreach ($gameRelations as $gameId => $relation) {
            if (in_array($gameId, $existingGameIds)) {
                $validGameRelations[$gameId] = $relation;
            } else {
                $invalidGameIds[] = $gameId;
            }
        }
        
        if (!empty($invalidGameIds)) {
            Log::warning('发现无效的游戏ID', [
                'invalid_game_ids' => $invalidGameIds,
                'invalid_count' => count($invalidGameIds)
            ]);
        }
        
        Log::info('游戏ID验证完成', [
            'total_game_ids' => count($gameIds),
            'valid_game_ids' => count($validGameRelations),
            'invalid_game_ids' => count($invalidGameIds)
        ]);
        
        return $validGameRelations;
    }
    
    /**
     * 批量插入游戏关系数据
     */
    private function batchInsertGameRelations($gameRelations)
    {
        Log::info('开始批量插入游戏关系数据');
        
        if (empty($gameRelations)) {
            return 0;
        }
        
        // 准备插入数据
        $insertData = [];
        foreach ($gameRelations as $relation) {
            $insertData[] = [
                'game_id' => $relation['game_id'],
                'show_group_prefix' => $relation['show_group_prefix'],
                'run_group_prefix' => $relation['run_group_prefix']
            ];
        }
        
        // 分批插入（每批1000条）
        $batchSize = 1000;
        $totalInserted = 0;
        $batches = array_chunk($insertData, $batchSize);
        
        foreach ($batches as $batchIndex => $batch) {
            try {
                $inserted = Db::table('ntp_api_group_games')->insertAll($batch);
                $totalInserted += count($batch);
                
                Log::debug('批次插入完成', [
                    'batch_index' => $batchIndex + 1,
                    'batch_size' => count($batch),
                    'total_inserted' => $totalInserted
                ]);
                
            } catch (\Exception $e) {
                Log::error('批次插入失败', [
                    'batch_index' => $batchIndex + 1,
                    'error' => $e->getMessage(),
                    'batch_data' => $batch
                ]);
                throw $e;
            }
        }
        
        Log::info('批量插入游戏关系数据完成', [
            'total_batches' => count($batches),
            'total_inserted' => $totalInserted
        ]);
        
        return $totalInserted;
    }
}