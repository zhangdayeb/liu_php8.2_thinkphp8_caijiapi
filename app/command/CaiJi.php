<?php
declare (strict_types = 1);

namespace app\command;

use think\console\Command;
use think\console\Input;
use think\console\input\Option;
use think\console\Output;
use think\facade\Db;
use think\facade\Log;

class CaiJi extends Command
{
    // 采集配置
    protected $config = [];
    protected $typeIdMap = [];
    protected $baseUrl = '';
    protected $comeKey = '';
    
    // 统计信息
    protected $totalInserted = 0;
    protected $totalFailed = 0;
    protected $startTime = 0;
    
    // 输出对象
    protected $output;
    
    protected function configure()
    {
        $this->setName('caiji')
            ->setDescription('采集视频资源到数据库')
            ->addOption('page', 'p', Option::VALUE_OPTIONAL, '只采集前N页', 0)
            ->addOption('continue', 'c', Option::VALUE_NONE, '从断点继续采集')
            ->addOption('test', 't', Option::VALUE_NONE, '测试模式，只采集1页')
            ->addOption('clear', null, Option::VALUE_NONE, '清空数据后重新采集');
    }

    protected function execute(Input $input, Output $output)
    {
        $this->output = $output;
        $this->startTime = time();
        
        // 设置执行时间和内存限制
        set_time_limit(0);
        ini_set('memory_limit', '512M');
        
        $output->writeln('<info>========================================</info>');
        $output->writeln('<info>视频资源采集程序启动</info>');
        $output->writeln('<info>开始时间：' . date('Y-m-d H:i:s') . '</info>');
        $output->writeln('<info>========================================</info>');
        
        try {
            // 1. 读取配置
            $this->loadConfig($output);
            
            // 2. 处理参数
            $pageLimit = $input->getOption('page') ? intval($input->getOption('page')) : 0;
            $isContinue = $input->getOption('continue');
            $isTest = $input->getOption('test');
            $isClear = $input->getOption('clear');
            
            if ($isTest) {
                $pageLimit = 1;
                $output->writeln('<comment>测试模式：只采集1页数据</comment>');
            }
            
            // 3. 清空数据表（如果需要）
            if ($isClear || (!$isContinue && !$isTest)) {
                $this->clearVideoTable($output);
            }
            
            // 4. 获取起始页码
            $startPage = 1;
            if ($isContinue) {
                $startPage = $this->getLastPage();
                $output->writeln('<comment>断点续传模式：从第' . $startPage . '页开始</comment>');
            }
            
            // 5. 开始采集
            $this->startCollection($startPage, $pageLimit);
            
            // 6. 输出统计信息
            $this->showStatistics();
            
        } catch (\Exception $e) {
            $output->writeln('<error>采集失败：' . $e->getMessage() . '</error>');
            Log::error('采集失败', [
                'error' => $e->getMessage(),
                'file' => $e->getFile(),
                'line' => $e->getLine()
            ]);
            return 1;
        }
        
        return 0;
    }
    
    /**
     * 加载采集配置
     */
    private function loadConfig(Output $output)
    {
        $output->writeln('正在加载采集配置...');
        
        $config = Db::table('ntp_caiji')->where('id', 1)->find();
        if (!$config) {
            throw new \Exception('未找到采集配置');
        }
        
        $this->config = $config;
        $this->comeKey = $config['come_key'];
        $this->baseUrl = rtrim($config['base_url'], '/');
        $this->typeIdMap = json_decode($config['type_id_translate'], true) ?: [];
        
        $output->writeln('采集源：' . $this->comeKey);
        $output->writeln('基础URL：' . $this->baseUrl);
        $output->writeln('类型映射数：' . count($this->typeIdMap));
    }
    
    /**
     * 清空视频表
     */
    private function clearVideoTable(Output $output)
    {
        $output->writeln('正在清空视频表...');
        $count = Db::table('ntp_video')->count();
        Db::table('ntp_video')->delete(true);
        $output->writeln('<info>已清空 ' . $count . ' 条旧数据</info>');
    }
    
    /**
     * 获取断点页码
     */
    private function getLastPage(): int
    {
        $stateInfo = $this->config['caiji_state_info'] ?? '';
        if ($stateInfo) {
            $state = json_decode($stateInfo, true);
            return isset($state['current_page']) ? $state['current_page'] : 1;
        }
        return 1;
    }
    
    /**
     * 开始采集
     */
    private function startCollection(int $startPage, int $pageLimit)
    {
        $this->output->writeln('开始采集视频数据...');
        
        // 首先获取第一页，确定总页数
        $firstPageData = $this->fetchListPage(1);
        if (!$firstPageData) {
            throw new \Exception('无法获取列表数据');
        }
        
        $totalPages = $firstPageData['pagecount'] ?? 1;
        $this->output->writeln('总页数：' . $totalPages);
        $this->output->writeln('总记录数：' . ($firstPageData['total'] ?? 0));
        
        // 确定实际要采集的页数
        $endPage = $pageLimit > 0 ? min($startPage + $pageLimit - 1, $totalPages) : $totalPages;
        
        $this->output->writeln('采集范围：第' . $startPage . '页 至 第' . $endPage . '页');
        $this->output->writeln('----------------------------------------');
        
        // 逐页采集
        for ($page = $startPage; $page <= $endPage; $page++) {
            $this->output->write('[' . date('H:i:s') . '] 正在采集第 ' . $page . '/' . $endPage . ' 页...');
            
            // 获取列表数据
            $listData = $page == 1 ? $firstPageData : $this->fetchListPage($page);
            if (!$listData || empty($listData['list'])) {
                $this->output->writeln(' <error>失败</error>');
                continue;
            }
            
            // 收集视频ID
            $vodIds = array_column($listData['list'], 'vod_id');
            $this->output->write(' 获取到 ' . count($vodIds) . ' 个视频ID...');
            
            // 批量获取详情并保存
            $savedCount = $this->fetchAndSaveDetails($vodIds);
            
            $this->output->writeln(' <info>成功保存 ' . $savedCount . ' 条</info>');
            
            // 更新进度
            $this->updateProgress($page, $totalPages);
            
            // 每10页输出一次统计
            if ($page % 10 == 0) {
                $this->output->writeln('<comment>已采集 ' . $this->totalInserted . ' 条，失败 ' . $this->totalFailed . ' 条</comment>');
            }
            
            // 随机延迟 5-10 秒
            if ($page < $endPage) {
                $sleep = rand(5, 10);
                $this->output->writeln('  等待 ' . $sleep . ' 秒后继续...');
                sleep($sleep);
            }
        }
    }
    
    /**
     * 获取列表页数据
     */
    private function fetchListPage(int $page): ?array
    {
        $url = $this->baseUrl . '/api.php/provide/vod/?ac=list&pg=' . $page;
        
        try {
            $response = $this->httpGet($url);
            if (!$response) {
                return null;
            }
            
            $data = json_decode($response, true);
            if (json_last_error() !== JSON_ERROR_NONE || !isset($data['code']) || $data['code'] != 1) {
                return null;
            }
            
            return $data;
            
        } catch (\Exception $e) {
            Log::error('获取列表页失败', [
                'page' => $page,
                'error' => $e->getMessage()
            ]);
            return null;
        }
    }
    
    /**
     * 批量获取详情并保存
     */
    private function fetchAndSaveDetails(array $vodIds): int
    {
        $savedCount = 0;
        
        // 分批获取详情，每批10个
        $chunks = array_chunk($vodIds, 10);
        
        foreach ($chunks as $chunk) {
            // 随机延迟 5-10 秒
            sleep(rand(5, 10));
            
            $ids = implode(',', $chunk);
            $url = $this->baseUrl . '/api.php/provide/vod/?ac=detail&ids=' . $ids;
            
            try {
                $response = $this->httpGet($url);
                if (!$response) {
                    $this->totalFailed += count($chunk);
                    continue;
                }
                
                $data = json_decode($response, true);
                if (json_last_error() !== JSON_ERROR_NONE || !isset($data['list'])) {
                    $this->totalFailed += count($chunk);
                    continue;
                }
                
                // 保存数据
                foreach ($data['list'] as $vod) {
                    if ($this->saveVideo($vod)) {
                        $savedCount++;
                        $this->totalInserted++;
                    } else {
                        $this->totalFailed++;
                    }
                }
                
            } catch (\Exception $e) {
                Log::error('获取详情失败', [
                    'ids' => $ids,
                    'error' => $e->getMessage()
                ]);
                $this->totalFailed += count($chunk);
            }
        }
        
        return $savedCount;
    }
    
    /**
     * 保存单个视频
     */
    private function saveVideo(array $vod): bool
    {
        try {
            // 映射类型ID
            $typeId = isset($this->typeIdMap[$vod['type_id']]) 
                ? $this->typeIdMap[$vod['type_id']] 
                : 1;
            
            // 准备数据
            $data = [
                'come_key' => $this->comeKey,
                'caiji_key' => 'mt_' . $vod['vod_id'],
                'video_title' => $vod['vod_name'] ?? '',
                'video_img_url' => $vod['vod_pic'] ?? '',
                'video_describe' => strip_tags($vod['vod_content'] ?? $vod['vod_blurb'] ?? ''),
                'video_info' => $vod['vod_play_url'] ?? '',
                'type_id' => $typeId,
                'list_order' => 0,
                'play_times' => 0
            ];
            
            // 插入或更新
            $exists = Db::table('ntp_video')
                ->where('caiji_key', $data['caiji_key'])
                ->find();
            
            if ($exists) {
                // 更新
                Db::table('ntp_video')
                    ->where('id', $exists['id'])
                    ->update($data);
            } else {
                // 插入
                Db::table('ntp_video')->insert($data);
            }
            
            return true;
            
        } catch (\Exception $e) {
            Log::error('保存视频失败', [
                'vod_id' => $vod['vod_id'],
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }
    
    /**
     * 更新采集进度
     */
    private function updateProgress(int $currentPage, int $totalPages)
    {
        $state = [
            'current_page' => $currentPage,
            'total_page' => $totalPages,
            'last_time' => date('Y-m-d H:i:s'),
            'total_inserted' => $this->totalInserted,
            'total_failed' => $this->totalFailed
        ];
        
        Db::table('ntp_caiji')
            ->where('id', 1)
            ->update([
                'caiji_state_info' => json_encode($state)
            ]);
    }
    
    /**
     * HTTP GET请求
     */
    private function httpGet(string $url, int $retry = 3): ?string
    {
        for ($i = 0; $i < $retry; $i++) {
            try {
                $ch = curl_init();
                curl_setopt($ch, CURLOPT_URL, $url);
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                curl_setopt($ch, CURLOPT_TIMEOUT, 30);
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
                curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
                curl_setopt($ch, CURLOPT_USERAGENT, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
                
                $response = curl_exec($ch);
                $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                curl_close($ch);
                
                if ($httpCode == 200 && $response) {
                    return $response;
                }
                
            } catch (\Exception $e) {
                Log::error('HTTP请求异常', ['url' => $url, 'error' => $e->getMessage()]);
            }
            
            if ($i < $retry - 1) {
                sleep(2); // 重试间隔
            }
        }
        
        return null;
    }
    
    /**
     * 显示统计信息
     */
    private function showStatistics()
    {
        $endTime = time();
        $duration = $endTime - $this->startTime;
        
        $this->output->writeln('');
        $this->output->writeln('<info>========================================</info>');
        $this->output->writeln('<info>采集完成！</info>');
        $this->output->writeln('<info>----------------------------------------</info>');
        $this->output->writeln('<info>成功采集：' . $this->totalInserted . ' 条</info>');
        $this->output->writeln('<info>失败数量：' . $this->totalFailed . ' 条</info>');
        $this->output->writeln('<info>总耗时：' . $this->formatTime($duration) . '</info>');
        $this->output->writeln('<info>结束时间：' . date('Y-m-d H:i:s') . '</info>');
        $this->output->writeln('<info>========================================</info>');
    }
    
    /**
     * 格式化时间
     */
    private function formatTime(int $seconds): string
    {
        $hours = floor($seconds / 3600);
        $minutes = floor(($seconds % 3600) / 60);
        $secs = $seconds % 60;
        
        if ($hours > 0) {
            return sprintf('%d小时%d分钟%d秒', $hours, $minutes, $secs);
        } elseif ($minutes > 0) {
            return sprintf('%d分钟%d秒', $minutes, $secs);
        } else {
            return sprintf('%d秒', $secs);
        }
    }
}