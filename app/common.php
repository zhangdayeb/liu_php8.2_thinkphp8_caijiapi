<?php
// 应用公共文件
use think\facade\Log;

/*
 * 生成 UUID v4
 * @return string
 */
function generateUuid(): string
{
    $data = random_bytes(16);

    // 设置版本号（4）和变体（10xxxxxx）
    $data[6] = chr((ord($data[6]) & 0x0f) | 0x40); // version 4
    $data[8] = chr((ord($data[8]) & 0x3f) | 0x80); // variant

    $uuid = vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    
    Log::debug('generateUuid - UUID生成');
    Log::debug('generateUuid - UUID: ' . $uuid);
    Log::debug('generateUuid - 时间戳: ' . date('Y-m-d H:i:s'));
    
    return $uuid;
}

/*
 * 生成 API 签名
 * @param string $traceId 替换用 UUID
 * @param string $requestBody 原始 JSON 字符串，包含 {{traceId}} 占位符
 * @return string 签名（HMAC-SHA256）
 */
function generateApiSignature(string $traceId, string $requestBody): string
{
    Log::debug('generateApiSignature - 开始生成API签名');
    Log::debug('generateApiSignature - TraceId: ' . $traceId);
    Log::debug('generateApiSignature - 请求体长度: ' . strlen($requestBody));
    Log::debug('generateApiSignature - 时间戳: ' . date('Y-m-d H:i:s'));
    
    $apiSecret = env('X-API-Secret');
    
    // 检查API密钥是否存在
    if (empty($apiSecret)) {
        Log::error('generateApiSignature - API密钥未配置');
        Log::error('generateApiSignature - TraceId: ' . $traceId);
        Log::error('generateApiSignature - 环境变量: X-API-Secret');
        throw new \Exception('API密钥未配置');
    }
    
    // 替换 traceId 占位符
    $jsonWithTraceId = str_replace('{{traceId}}', $traceId, $requestBody);
    
    Log::debug('generateApiSignature - JSON占位符替换完成');
    Log::debug('generateApiSignature - TraceId: ' . $traceId);
    Log::debug('generateApiSignature - 原始长度: ' . strlen($requestBody));
    Log::debug('generateApiSignature - 替换后长度: ' . strlen($jsonWithTraceId));
    Log::debug('generateApiSignature - 是否包含占位符: ' . (strpos($requestBody, '{{traceId}}') !== false ? '是' : '否'));
    
    // 生成签名
    $signature = hash_hmac('sha256', $jsonWithTraceId, $apiSecret);
    
    Log::info('generateApiSignature - API签名生成成功');
    Log::info('generateApiSignature - TraceId: ' . $traceId);
    Log::info('generateApiSignature - 签名前缀: ' . substr($signature, 0, 8) . '...');
    Log::info('generateApiSignature - 签名长度: ' . strlen($signature));
    Log::info('generateApiSignature - 数据长度: ' . strlen($jsonWithTraceId));
    
    return $signature;
}

/**
 * 发送API请求
 * @param string $endpoint 接口端点 (例如: /game/vendors)
 * @param array $data 请求数据
 * @param string $method 请求方法 GET|POST
 * @return array 响应结果
 */
function sendApiRequest($endpoint, $data = [], $method = 'POST')
{
    // 生成追踪ID
    $traceId = generateUuid();
    
    Log::info('sendApiRequest - API请求开始');
    Log::info('sendApiRequest - 端点: ' . $endpoint);
    Log::info('sendApiRequest - 方法: ' . strtoupper($method));
    Log::info('sendApiRequest - TraceId: ' . $traceId);
    Log::info('sendApiRequest - 请求数据数量: ' . count($data));
    Log::info('sendApiRequest - 时间戳: ' . date('Y-m-d H:i:s'));
    
    // 记录请求数据（敏感信息脱敏）
    $logData = $data;
    if (isset($logData['password'])) $logData['password'] = '***';
    if (isset($logData['token'])) $logData['token'] = substr($logData['token'], 0, 8) . '...';
    
    Log::debug('sendApiRequest - 请求数据详情');
    Log::debug('sendApiRequest - TraceId: ' . $traceId);
    Log::debug('sendApiRequest - 请求数据: ' . json_encode($logData));
    
    try {
        // 准备请求数据 | traceId 是任何请求都必填选项
        $requestData = array_merge($data, ['traceId' => $traceId]);
        $requestBody = json_encode($requestData, JSON_UNESCAPED_UNICODE);
        
        if (json_last_error() !== JSON_ERROR_NONE) {
            Log::error('sendApiRequest - JSON编码失败');
            Log::error('sendApiRequest - TraceId: ' . $traceId);
            Log::error('sendApiRequest - JSON错误: ' . json_last_error_msg());
            Log::error('sendApiRequest - 请求数据: ' . json_encode($requestData));
            
            return [
                'success' => false,
                'error' => 'JSON编码失败: ' . json_last_error_msg(),
                'http_code' => 0,
                'trace_id' => $traceId
            ];
        }
        
        Log::debug('sendApiRequest - 请求体准备完成');
        Log::debug('sendApiRequest - TraceId: ' . $traceId);
        Log::debug('sendApiRequest - 请求体长度: ' . strlen($requestBody));
        Log::debug('sendApiRequest - 请求体预览: ' . substr($requestBody, 0, 200) . (strlen($requestBody) > 200 ? '...' : ''));
        
        // 生成签名
        $signature = generateApiSignature($traceId, $requestBody);
        
        // 构建完整URL
        $baseUrl = env('API_URL');
        if (empty($baseUrl)) {
            Log::error('sendApiRequest - API基础URL未配置');
            Log::error('sendApiRequest - TraceId: ' . $traceId);
            Log::error('sendApiRequest - 环境变量: API_URL');
            
            return [
                'success' => false,
                'error' => 'API基础URL未配置',
                'http_code' => 0,
                'trace_id' => $traceId
            ];
        }
        
        $url = rtrim($baseUrl, '/') . '/' . ltrim($endpoint, '/');
        
        Log::debug('sendApiRequest - URL构建完成');
        Log::debug('sendApiRequest - TraceId: ' . $traceId);
        Log::debug('sendApiRequest - 基础URL: ' . $baseUrl);
        Log::debug('sendApiRequest - 端点: ' . $endpoint);
        Log::debug('sendApiRequest - 完整URL: ' . $url);
        
        // 初始化cURL
        $ch = curl_init();
        if (!$ch) {
            Log::error('sendApiRequest - cURL初始化失败');
            Log::error('sendApiRequest - TraceId: ' . $traceId);
            
            return [
                'success' => false,
                'error' => 'cURL初始化失败',
                'http_code' => 0,
                'trace_id' => $traceId
            ];
        }
        
        // 设置请求头
        $apiKey = env('X-API-Key');
        if (empty($apiKey)) {
            Log::error('sendApiRequest - API密钥未配置');
            Log::error('sendApiRequest - TraceId: ' . $traceId);
            Log::error('sendApiRequest - 环境变量: X-API-Key');
            
            curl_close($ch);
            return [
                'success' => false,
                'error' => 'API密钥未配置',
                'http_code' => 0,
                'trace_id' => $traceId
            ];
        }
        
        $headers = [
            'Content-Type: application/json',
            'X-API-Key: ' . $apiKey,
            'X-Signature: ' . $signature
        ];
        
        Log::debug('sendApiRequest - 请求头设置完成');
        Log::debug('sendApiRequest - TraceId: ' . $traceId);
        Log::debug('sendApiRequest - 请求头数量: ' . count($headers));
        Log::debug('sendApiRequest - API密钥前缀: ' . substr($apiKey, 0, 8) . '...');
        Log::debug('sendApiRequest - 签名前缀: ' . substr($signature, 0, 8) . '...');
        
        // 基础cURL设置
        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => $headers,
            CURLOPT_TIMEOUT => 30,
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false,
        ]);
        
        // 根据请求方法设置
        if (strtoupper($method) === 'POST') {
            curl_setopt($ch, CURLOPT_POST, true);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $requestBody);
            
            Log::debug('sendApiRequest - POST请求配置完成');
            Log::debug('sendApiRequest - TraceId: ' . $traceId);
            Log::debug('sendApiRequest - POST数据长度: ' . strlen($requestBody));
        } elseif (strtoupper($method) === 'GET' && !empty($data)) {
            $queryString = http_build_query($data);
            $url .= '?' . $queryString;
            curl_setopt($ch, CURLOPT_URL, $url);
            
            Log::debug('sendApiRequest - GET请求配置完成');
            Log::debug('sendApiRequest - TraceId: ' . $traceId);
            Log::debug('sendApiRequest - 查询字符串: ' . $queryString);
            Log::debug('sendApiRequest - 最终URL: ' . $url);
        }
        
        // 记录请求开始时间
        $startTime = microtime(true);
        
        Log::info('sendApiRequest - 开始执行HTTP请求');
        Log::info('sendApiRequest - TraceId: ' . $traceId);
        Log::info('sendApiRequest - URL: ' . $url);
        Log::info('sendApiRequest - 方法: ' . strtoupper($method));
        Log::info('sendApiRequest - 开始时间: ' . date('Y-m-d H:i:s', (int)$startTime));
        
        // 执行请求
        $response = curl_exec($ch);
        $endTime = microtime(true);
        $duration = round(($endTime - $startTime) * 1000, 2); // 毫秒
        
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        $curlInfo = curl_getinfo($ch);
        
        curl_close($ch);
        
        Log::info('sendApiRequest - HTTP请求执行完成');
        Log::info('sendApiRequest - TraceId: ' . $traceId);
        Log::info('sendApiRequest - HTTP状态码: ' . $httpCode);
        Log::info('sendApiRequest - 耗时(毫秒): ' . $duration);
        Log::info('sendApiRequest - 响应长度: ' . ($response ? strlen($response) : 0));
        Log::info('sendApiRequest - 是否有cURL错误: ' . (!empty($error) ? '是' : '否'));
        Log::info('sendApiRequest - 总时间: ' . ($curlInfo['total_time'] ?? 'null'));
        Log::info('sendApiRequest - 连接时间: ' . ($curlInfo['connect_time'] ?? 'null'));
        
        // 检查cURL错误
        if ($error) {
            Log::error('sendApiRequest - cURL执行错误');
            Log::error('sendApiRequest - TraceId: ' . $traceId);
            Log::error('sendApiRequest - cURL错误: ' . $error);
            Log::error('sendApiRequest - cURL错误代码: ' . curl_errno($ch));
            Log::error('sendApiRequest - 耗时(毫秒): ' . $duration);
            Log::error('sendApiRequest - URL: ' . $url);
            
            return [
                'success' => false,
                'error' => 'cURL Error: ' . $error,
                'http_code' => 0,
                'trace_id' => $traceId,
                'duration_ms' => $duration
            ];
        }
        
        // 检查HTTP响应
        if ($response === false) {
            Log::error('sendApiRequest - HTTP响应为空');
            Log::error('sendApiRequest - TraceId: ' . $traceId);
            Log::error('sendApiRequest - HTTP状态码: ' . $httpCode);
            Log::error('sendApiRequest - 耗时(毫秒): ' . $duration);
            
            return [
                'success' => false,
                'error' => 'HTTP响应为空',
                'http_code' => $httpCode,
                'trace_id' => $traceId,
                'duration_ms' => $duration
            ];
        }
        
        Log::debug('sendApiRequest - HTTP响应接收成功');
        Log::debug('sendApiRequest - TraceId: ' . $traceId);
        Log::debug('sendApiRequest - 响应长度: ' . strlen($response));
        Log::debug('sendApiRequest - 响应预览: ' . substr($response, 0, 200) . (strlen($response) > 200 ? '...' : ''));
        Log::debug('sendApiRequest - Content-Type: ' . ($curlInfo['content_type'] ?? 'null'));
        
        // 解析响应
        $responseData = json_decode($response, true);
        
        if (json_last_error() !== JSON_ERROR_NONE) {
            Log::warning('sendApiRequest - JSON响应解析失败');
            Log::warning('sendApiRequest - TraceId: ' . $traceId);
            Log::warning('sendApiRequest - JSON错误: ' . json_last_error_msg());
            Log::warning('sendApiRequest - 响应预览: ' . substr($response, 0, 500));
            Log::warning('sendApiRequest - HTTP状态码: ' . $httpCode);
            
            // JSON解析失败时，responseData设为null，但不算完全失败
            $responseData = null;
        } else {
            Log::debug('sendApiRequest - JSON响应解析成功');
            Log::debug('sendApiRequest - TraceId: ' . $traceId);
            Log::debug('sendApiRequest - 响应数据类型: ' . gettype($responseData));
            Log::debug('sendApiRequest - 响应键名: ' . (is_array($responseData) ? implode(', ', array_keys($responseData)) : 'null'));
        }
        
        $result = [
            'success' => $httpCode === 200,
            'http_code' => $httpCode,
            'data' => $responseData,
            'raw_response' => $response,
            'trace_id' => $traceId,
            'duration_ms' => $duration
        ];
        
        // 记录最终结果
        if ($result['success']) {
            Log::info('sendApiRequest - API请求成功完成');
            Log::info('sendApiRequest - TraceId: ' . $traceId);
            Log::info('sendApiRequest - 端点: ' . $endpoint);
            Log::info('sendApiRequest - HTTP状态码: ' . $httpCode);
            Log::info('sendApiRequest - 耗时(毫秒): ' . $duration);
            Log::info('sendApiRequest - 响应数据可用: ' . ($responseData !== null ? '是' : '否'));
        } else {
            Log::warning('sendApiRequest - API请求未成功');
            Log::warning('sendApiRequest - TraceId: ' . $traceId);
            Log::warning('sendApiRequest - 端点: ' . $endpoint);
            Log::warning('sendApiRequest - HTTP状态码: ' . $httpCode);
            Log::warning('sendApiRequest - 耗时(毫秒): ' . $duration);
            Log::warning('sendApiRequest - 响应预览: ' . substr($response, 0, 200));
        }
        
        return $result;
        
    } catch (\Exception $e) {
        Log::error('sendApiRequest - API请求发生异常');
        Log::error('sendApiRequest - TraceId: ' . $traceId);
        Log::error('sendApiRequest - 端点: ' . $endpoint);
        Log::error('sendApiRequest - 异常信息: ' . $e->getMessage());
        Log::error('sendApiRequest - 异常文件: ' . $e->getFile());
        Log::error('sendApiRequest - 异常行号: ' . $e->getLine());
        Log::error('sendApiRequest - 异常堆栈: ' . $e->getTraceAsString());
        
        return [
            'success' => false,
            'error' => 'Exception: ' . $e->getMessage(),
            'http_code' => 0,
            'trace_id' => $traceId
        ];
    }
}

/**
 * 翻译API状态码为中文描述
 * @param string $statusCode 状态码
 * @return string 中文描述
 */
function translateStatusCode(string $statusCode): string
{
    Log::debug('translateStatusCode - 开始翻译状态码');
    Log::debug('translateStatusCode - 状态码: ' . $statusCode);
    Log::debug('translateStatusCode - 时间戳: ' . date('Y-m-d H:i:s'));
    
    // 状态码映射表
    $statusCodeMap = [
        'SC_OK' => '成功响应',
        'SC_UNKNOWN_ERROR' => '未知错误的通用状态代码',
        'SC_INVALID_REQUEST' => '请求体中发送的参数有错误/缺失',
        'SC_AUTHENTICATION_FAILED' => '验证失败。X-API-Key 丢失或无效',
        'SC_INVALID_SIGNATURE' => 'X-Signature 验证失败',
        'SC_INVALID_TOKEN' => '运营商系统中的令牌无效',
        'SC_INVALID_GAME' => '游戏无效',
        'SC_DUPLICATE_REQUEST' => '重复的请求',
        'SC_CURRENCY_NOT_SUPPORTED' => '不支持该货币',
        'SC_WRONG_CURRENCY' => '交易的货币与用户的钱包货币不同',
        'SC_INSUFFICIENT_FUNDS' => '用户的钱包资金不足',
        'SC_USER_NOT_EXISTS' => '在运营商系统中用户不存在',
        'SC_USER_DISABLED' => '用户已被禁用，不允许下注',
        'SC_TRANSACTION_DUPLICATED' => '发送了重复的交易ID',
        'SC_TRANSACTION_NOT_EXISTS' => '找不到相应的投注交易',
        'SC_VENDOR_ERROR' => '游戏供应商遇到错误',
        'SC_UNDER_MAINTENANCE' => '游戏正在维护中',
        'SC_MISMATCHED_DATA_TYPE' => '数据类型无效',
        'SC_INVALID_RESPONSE' => '响应无效',
        'SC_INVALID_VENDOR' => '不支持该供应商',
        'SC_INVALID_LANGUAGE' => '不支持该语言',
        'SC_GAME_DISABLED' => '游戏已被禁用',
        'SC_INVALID_PLATFORM' => '不支持该平台',
        'SC_GAME_LANGUAGE_NOT_SUPPORTED' => '不支持该游戏语言',
        'SC_GAME_PLATFORM_NOT_SUPPORTED' => '不支持该游戏平台',
        'SC_GAME_CURRENCY_NOT_SUPPORTED' => '不支持该游戏货币',
        'SC_VENDOR_LINE_DISABLED' => '游戏供应商线路已被禁用',
        'SC_VENDOR_CURRENCY_NOT_SUPPORTED' => '不支持该游戏供应商货币',
        'SC_VENDOR_LANGUAGE_NOT_SUPPORTED' => '不支持该游戏供应商语言',
        'SC_VENDOR_PLATFORM_NOT_SUPPORTED' => '不支持该游戏供应商平台',
        'SC_TRANSACTION_STILL_PROCESSING' => '交易仍在处理中，请稍后重试',
        'SC_EXCEEDED_NUMBER_OF_RETRIES' => '超出重试次数',
        'SC_OPERATOR_TIMEOUT' => '运营商已超时',
        'SC_INVALID_FROM_TIME' => '数据仅可获取最近60天',
        'SC_INVALID_DATE_RANGE' => '日期范围应该在一天之内',
        'SC_REFERENCE_ID_DUPLICATED' => '已发送重复的参考编号 Reference ID',
        'SC_TRANSACTION_DOES_NOT_EXIST' => '无法找到相应的参考编号 Reference ID',
        'SC_INTERNAL_ERROR' => '内部错误。请在相关客服渠道进行检查',
        'SC_WALLET_NOT_SUPPORTED' => '不支持该钱包类型'
    ];
    
    // 查找对应的中文描述
    $translation = $statusCodeMap[$statusCode] ?? null;
    
    if ($translation !== null) {
        Log::debug('translateStatusCode - 状态码翻译成功');
        Log::debug('translateStatusCode - 状态码: ' . $statusCode);
        Log::debug('translateStatusCode - 翻译: ' . $translation);
        
        return $translation;
    } else {
        Log::warning('translateStatusCode - 未找到状态码对应的翻译');
        Log::warning('translateStatusCode - 状态码: ' . $statusCode);
        Log::warning('translateStatusCode - 可用状态码数量: ' . count($statusCodeMap));
        
        return '未知状态码: ' . $statusCode;
    }
}

/**
 * 验证API请求签名
 * @param string $requestBody 原始请求体JSON字符串
 * @param string $signature 请求头中的X-Signature值
 * @return bool 验证结果
 */
function verifyApiSignature(string $requestBody, string $signature): bool
{
    Log::debug('verifyApiSignature - 开始验证API请求签名');
    Log::debug('verifyApiSignature - 请求体长度: ' . strlen($requestBody));
    Log::debug('verifyApiSignature - 签名前缀: ' . substr($signature, 0, 8) . '...');
    Log::debug('verifyApiSignature - 时间戳: ' . date('Y-m-d H:i:s'));
    
    try {
        // 检查签名是否为空
        if (empty($signature)) {
            Log::warning('verifyApiSignature - 签名为空');
            return false;
        }
        
        // 检查请求体是否为空
        if (empty($requestBody)) {
            Log::warning('verifyApiSignature - 请求体为空');
            return false;
        }
        
        // 获取API密钥
        $apiSecret = env('X-API-Secret');
        if (empty($apiSecret)) {
            Log::error('verifyApiSignature - API密钥未配置');
            Log::error('verifyApiSignature - 环境变量: X-API-Secret');
            return false;
        }
        
        Log::debug('verifyApiSignature - API密钥获取成功');
        Log::debug('verifyApiSignature - API密钥前缀: ' . substr($apiSecret, 0, 8) . '...');
        
        // 生成预期的签名
        $expectedSignature = hash_hmac('sha256', $requestBody, $apiSecret);
        
        Log::debug('verifyApiSignature - 预期签名生成完成');
        Log::debug('verifyApiSignature - 预期签名前缀: ' . substr($expectedSignature, 0, 8) . '...');
        Log::debug('verifyApiSignature - 接收签名前缀: ' . substr($signature, 0, 8) . '...');
        
        // 使用 hash_equals 进行安全的字符串比较，防止时序攻击
        $isValid = hash_equals($expectedSignature, $signature);
        
        if ($isValid) {
            Log::info('verifyApiSignature - API签名验证成功');
        } else {
            Log::warning('verifyApiSignature - API签名验证失败');
            Log::warning('verifyApiSignature - 预期签名前缀: ' . substr($expectedSignature, 0, 8) . '...');
            Log::warning('verifyApiSignature - 接收签名前缀: ' . substr($signature, 0, 8) . '...');
        }
        
        return $isValid;
        
    } catch (\Exception $e) {
        Log::error('verifyApiSignature - 签名验证过程发生异常');
        Log::error('verifyApiSignature - 错误信息: ' . $e->getMessage());
        Log::error('verifyApiSignature - 错误文件: ' . $e->getFile());
        Log::error('verifyApiSignature - 错误行号: ' . $e->getLine());
        
        return false;
    }
}
/**
 * 金额向下截取到指定小数位数（不四舍五入）
 * @param float $amount 原始金额
 * @param int $decimals 保留小数位数，默认2位
 * @return float 截取后的金额
 */
function moneyFloor(float $amount, int $decimals = 2): float
{
    // 计算倍数
    $multiplier = pow(10, $decimals);
    
    // 修复：先round到更高精度，再floor，避免浮点数精度问题
    $rounded = round($amount * $multiplier, 0, PHP_ROUND_HALF_DOWN);
    $result = floor($rounded) / $multiplier;
    
    Log::debug('moneyFloor - 金额向下截取');
    Log::debug('moneyFloor - 原始金额: ' . $amount);
    Log::debug('moneyFloor - 保留位数: ' . $decimals);
    Log::debug('moneyFloor - 截取结果: ' . $result);
    
    return $result;
}

/**
 * 格式化金额为字符串（保留2位小数）
 * @param float $amount 金额
 * @return string 格式化后的金额字符串
 */
function formatMoney(float $amount): string
{
    // 先进行精度修正，再格式化
    $correctedAmount = round($amount, 2);
    return number_format($correctedAmount, 2, '.', '');
}
/**
 * ************************************************************************************
 * ************************************************************************************
 * 
 * 伟大的分割线
 * 
 * ************************************************************************************
 * ************************************************************************************ 
 */