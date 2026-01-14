// 공용 ApiResponse
export interface ApiResponse<T> {
  success: boolean;
  message: string;
  errorCode?: string;
  data: T;
}

// Campaign (캠페인)
export interface Campaign {
  id: number;
  name: string;
  totalStock: number;
  currentStock: number;
  status: 'OPEN' | 'CLOSED';
  createdAt: string;
  updatedAt: string;
}

// Campaign 생성 요청
export interface CreateCampaignRequest {
  name: string;
  totalStock: number;
}

// Participation 요청
export interface ParticipationRequest {
  userId: number;
}

// ParticipationHistory (참여 이력)
export interface ParticipationHistory {
  id: number;
  campaignId: number;
  userId: number;
  status: 'SUCCESS' | 'FAIL';
  createdAt: string;
}

// CampaignStats (캠페인 통계)
export interface CampaignStats {
  id: number;
  campaignId: number;
  statsDate: string;
  successCount: number;
  failCount: number;
  createdAt: string;
  updatedAt: string;
}

// DailyStats 응답
export interface DailyStatsResponse {
  date: string;
  summary?: {
    totalCampaigns: number;
    totalSuccess: number;
    totalFail: number;
    totalParticipation: number;
    overallSuccessRate: string;
  };
  message?: string;
  campaigns: Array<{
    campaignId: number;
    campaignName: string;
    successCount: number;
    failCount: number;
    totalCount: number;
    successRate: string;
    statsDate: string;
  }>;
}

// CampaignStats 응답
export interface CampaignStatsResponse {
  campaignId: number;
  campaignName: string;
  startDate: string;
  endDate: string;
  summary: {
    totalSuccess: number;
    totalFail: number;
    totalParticipation: number;
    averageSuccessRate: string;
  };
  dailyStats: Array<{
    date: string;
    successCount: number;
    failCount: number;
    totalCount: number;
    successRate: string;
  }>;
}

// BatchExecution (배치 실행)
export interface BatchExecution {
  jobExecutionId: number;
  jobName: string;
  status: 'STARTING' | 'STARTED' | 'COMPLETED' | 'FAILED' | 'STOPPED';
  exitStatus: string;
  exitDescription: string;
  startTime: string;
  endTime: string;
  targetDate: string;
  updatedRows?: string;
}

// BatchExecution 시작 응답
export interface BatchStartResponse {
  jobExecutionId: number;
  jobInstanceId: number;
  status: string;
  date: string;
  message: string;
}

// BatchHistory 응답
export interface BatchHistoryResponse {
  jobName: string;
  totalCount: number;
  history: Array<{
    jobInstanceId: number;
    jobName: string;
    jobExecutionId: number;
    status: string;
    exitStatus: string;
    startTime: string;
    endTime: string;
    targetDate: string;
    updatedRows: string;
  }>;
}

// --- 2025-12-30 추가된 타입 ---

// 실시간 캠페인 현황 (GET /api/campaigns/{id}/status)
export interface CampaignRealtimeStatus {
  campaignId: number;
  campaignName: string;
  totalStock: number;
  currentStock: number;
  successCount: number;
  failCount: number;
  totalParticipation: number;
  stockUsageRate: string;
  processingMetrics?: {
    actualTps: number;        // 실제 메시지 처리 TPS
    avgLatencyMs: number;      // 평균 처리 지연시간 (Kafka → DB)
    recentProcessed: number;   // 최근 5초간 처리된 메시지 수
  };
}

// 성능 비교용 통계 데이터 (공용 구조)
export interface PerformanceStats {
  date: string;
  method: 'RAW_QUERY' | 'BATCH_AGGREGATED';
  queryTimeMs: number;
  summary: {
    totalCampaigns: number;
    totalSuccess: number;
    totalFail: number;
    totalParticipation: number;
    overallSuccessRate: string;
  };
  campaigns: Array<{
    campaignId: number;
    campaignName: string;
    successCount: number;
    failCount: number;
    totalCount: number;
    successRate: string;
    statsDate?: string; // BATCH_AGGREGATED 에만 존재
  }>;
}

// 동기 방식 참여 응답 (POST /api/campaigns/{campaignId}/participation-sync)
export interface SyncParticipationResponse {
  status: 'SUCCESS' | 'FAIL';
  method: 'SYNC';
}

// --- Kafka 순서 분석 타입 (2025-01-02 추가) ---

// 파티션별 메시지 분포
export interface PartitionDistribution {
  partition: number;
  count: number;
}

// 순서 분석 응답 (GET /api/admin/stats/order-analysis/{campaignId})
export interface OrderAnalysisResponse {
  campaignId: number;
  queryTimeMs: number;
  summary: {
    totalRecords: number;
    orderMismatches: number;
    orderAccuracy: string;
  };
  partitionDistribution: PartitionDistribution[];
  partitionCount?: number; // 파티션 개수 (optional, 백엔드에서 전달 시)
}

// --- 처리 로그 타입 (2025-01-02 추가) ---

// 로그 엔트리
export interface LogEntry {
  timestamp: string;
  level: string;
  message: string;
}

// --- 순서 위반 타입 (2025-01-14 추가) ---

// 메시지 정보
export interface MessageInfo {
  userId: number;
  timestamp: string;
  partition: number;
  offset: number;
  processingSeq: number;
}

// 순서 위반 상세 정보
export interface OrderViolation {
  current: MessageInfo;
  next: MessageInfo;
  explanation: string;
}

// 순서 위반 응답 (GET /api/admin/stats/order-violations/{campaignId})
export interface OrderViolationResponse {
  campaignId: number;
  queryTimeMs: number;
  totalViolations: number;
  violations: OrderViolation[];
}
