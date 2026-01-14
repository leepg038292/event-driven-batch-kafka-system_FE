import apiClient from './client';
import type { PerformanceStats, CampaignStatsResponse, ApiResponse, OrderAnalysisResponse, OrderViolationResponse } from '../types/index';

// 일자별 배치 집계 통계 조회 (빠른 API)
export const getDailyPerformanceStats = async (date: string): Promise<PerformanceStats> => {
  const response = await apiClient.get<ApiResponse<PerformanceStats>>(
    `/api/admin/stats/daily?date=${date}`
  );
  return response.data.data;
};

// 일자별 원본 데이터 집계 통계 조회 (느린 API)
export const getRawPerformanceStats = async (date: string): Promise<PerformanceStats> => {
  const response = await apiClient.get<ApiResponse<PerformanceStats>>(
    `/api/admin/stats/raw?date=${date}`
  );
  return response.data.data;
};

// 캠페인별 기간 통계 조회
export const getCampaignStats = async (
  campaignId: number,
  startDate?: string,
  endDate?: string
): Promise<CampaignStatsResponse> => {
  let url = `/api/admin/stats/campaign/${campaignId}`;
  const params = new URLSearchParams();

  if (startDate) params.append('startDate', startDate);
  if (endDate) params.append('endDate', endDate);

  if (params.toString()) {
    url += `?${params.toString()}`;
  }

  const response = await apiClient.get<ApiResponse<CampaignStatsResponse>>(url);
  return response.data.data;
};

// Kafka 순서 분석 조회 (GET /api/admin/stats/order-analysis/{campaignId})
export const getOrderAnalysis = async (campaignId: number): Promise<OrderAnalysisResponse> => {
  const response = await apiClient.get<ApiResponse<any>>(
    `/api/admin/stats/order-analysis/${campaignId}`
  );

  const data = response.data.data;

  // 백엔드가 partitionDistribution을 객체 {"0": 201}로 반환하므로 배열로 변환
  const partitionDistribution = Object.entries(data.partitionDistribution || {}).map(
    ([partition, count]) => ({
      partition: parseInt(partition),
      count: count as number,
    })
  );

  return {
    ...data,
    partitionDistribution,
  };
};

// Kafka 순서 위반 조회 (GET /api/admin/stats/order-violations/{campaignId})
export const getOrderViolations = async (
  campaignId: number,
  limit?: number
): Promise<OrderViolationResponse> => {
  let url = `/api/admin/stats/order-violations/${campaignId}`;
  if (limit) {
    url += `?limit=${limit}`;
  }

  const response = await apiClient.get<ApiResponse<any>>(url);
  const data = response.data.data;

  // 백엔드 응답 구조: violationsFound → totalViolations로 변환
  return {
    campaignId: data.campaignId,
    queryTimeMs: data.queryTimeMs,
    totalViolations: data.violationsFound || 0,  // 백엔드는 violationsFound 사용
    violations: data.violations || [],
  };
};
