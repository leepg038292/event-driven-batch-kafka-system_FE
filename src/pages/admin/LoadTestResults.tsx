import { useState, useEffect } from 'react';
import {
  Box,
  Container,
  Typography,
  Button,
  Paper,
  Grid,
  Card,
  CardContent,
  CircularProgress,
  Alert,
  TextField,
  LinearProgress,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Divider,
} from '@mui/material';
import { PlayArrow, Speed, Timer, Analytics } from '@mui/icons-material';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell } from 'recharts';
import {
  executeKafkaTest,
  executeSyncTest,
  getLoadTestResult,
  type LoadTestResult,
} from '../../api/loadTest';
import { getOrderAnalysis, getOrderViolations } from '../../api/stats';
import type { OrderAnalysisResponse, OrderViolationResponse } from '../../types';
import { useToast } from '../../components/ToastProvider';

// 파이 차트 색상
const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82CA9D'];

const LoadTestResults = () => {
  const { showToast } = useToast();

  const [campaignId, setCampaignId] = useState('1');
  const [totalRequests, setTotalRequests] = useState('30000');

  const [kafkaResult, setKafkaResult] = useState<LoadTestResult | null>(null);
  const [syncResult, setSyncResult] = useState<LoadTestResult | null>(null);

  const [kafkaLoading, setKafkaLoading] = useState(false);
  const [syncLoading, setSyncLoading] = useState(false);

  // 실시간 진행률 추적
  const [kafkaProgress, setKafkaProgress] = useState(0);
  const [syncProgress, setSyncProgress] = useState(0);
  const [kafkaStartTime, setKafkaStartTime] = useState<number | null>(null);
  const [syncStartTime, setSyncStartTime] = useState<number | null>(null);

  // 순서 분석 상태
  const [orderAnalysis, setOrderAnalysis] = useState<OrderAnalysisResponse | null>(null);
  const [orderAnalysisLoading, setOrderAnalysisLoading] = useState(false);

  // 순서 위반 상태
  const [orderViolations, setOrderViolations] = useState<OrderViolationResponse | null>(null);
  const [orderViolationsLoading, setOrderViolationsLoading] = useState(false);

  // 파티션 비교용 데이터 (여러 테스트 결과 저장)
  interface PartitionTestResult {
    partitions: number;
    kafkaResult?: LoadTestResult;
    orderAnalysis?: OrderAnalysisResponse;
  }
  const [comparisonResults, setComparisonResults] = useState<PartitionTestResult[]>([]);

  // Kafka 테스트 실행
  const handleKafkaTest = async () => {
    try {
      setKafkaLoading(true);
      setKafkaProgress(0);
      setKafkaStartTime(Date.now());
      showToast('Kafka 부하 테스트를 시작합니다...', 'info');

      const { jobId } = await executeKafkaTest({
        campaignId: parseInt(campaignId),
        totalRequests: parseInt(totalRequests),
        partitions: 1, // 파티션은 EC2에서 수동 변경 (Spring Profile)
      });

      pollTestResult(jobId, setKafkaResult, setKafkaLoading, setKafkaProgress, parseInt(totalRequests));

    } catch (error) {
      showToast('테스트를 시작하는 중 오류가 발생했습니다.', 'error');
      setKafkaProgress(0);
    }
  };

  // 동기 테스트 실행
  const handleSyncTest = async () => {
    try {
      setSyncLoading(true);
      setSyncProgress(0);
      setSyncStartTime(Date.now());
      showToast('동기 방식 부하 테스트를 시작합니다...', 'info');

      const { jobId } = await executeSyncTest({
        campaignId: parseInt(campaignId),
        totalRequests: parseInt(totalRequests),
        partitions: 1, // 파티션은 EC2에서 수동 변경 (Spring Profile)
      });

      pollTestResult(jobId, setSyncResult, setSyncLoading, setSyncProgress, parseInt(totalRequests));

    } catch (error) {
      showToast('테스트를 시작하는 중 오류가 발생했습니다.', 'error');
      setSyncLoading(false);
      setSyncProgress(0);
    }
  };

  // 결과 폴링
  const pollTestResult = (
    jobId: string,
    setResult: (result: LoadTestResult) => void,
    setLoading: (loading: boolean) => void,
    setProgress: (progress: number) => void,
    totalRequests: number
  ) => {
    let retryCount = 0;

    // 요청 규모에 따라 동적으로 타임아웃 계산
    const calculateTimeout = (requests: number) => {
      if (requests <= 10000) return 300; // 5분
      if (requests <= 30000) return 450; // 7.5분
      if (requests <= 70000) return 600; // 10분
      if (requests <= 100000) return 900; // 15분
      if (requests <= 500000) return 1800; // 30분
      if (requests <= 1000000) return 3000; // 50분
      return 4500; // 75분 (최대)
    };

    const maxRetries = calculateTimeout(totalRequests); // 2초 간격 폴링

    // 예상 소요 시간 계산 (TPS 기준: Kafka ~200, Sync ~10)
    const estimatedTPS = 200; // Kafka 평균 TPS
    const estimatedDuration = (totalRequests / estimatedTPS) * 1000; // ms 단위

    const interval = setInterval(async () => {
      retryCount++; // 폴링 시도 횟수 증가 (성공/실패 관계없이)

      try {
        const result = await getLoadTestResult(jobId);

        if (result.status === 'COMPLETED') {
          clearInterval(interval);
          setResult(result);
          setLoading(false);
          setProgress(100);
          showToast('테스트가 완료되었습니다!', 'success');
        } else if (result.status === 'FAILED') {
          clearInterval(interval);
          setLoading(false);
          setProgress(0);
          showToast('테스트 실패: ' + result.error, 'error');
        } else {
          // RUNNING: 진행률 업데이트 (추정)
          const elapsed = retryCount * 2000; // 2초마다 폴링
          const progress = Math.min((elapsed / estimatedDuration) * 100, 95); // 최대 95%까지
          setProgress(progress);
        }
      } catch (error) {
        console.warn(`폴링 에러 (${retryCount}/${maxRetries}):`, error);

        // 최대 재시도 횟수 초과 시에만 중단
        if (retryCount >= maxRetries) {
          clearInterval(interval);
          setLoading(false);
          setProgress(0);
          showToast('테스트 결과 조회 타임아웃', 'error');
        }
        // maxRetries 미만이면 계속 재시도 (2초 후 자동 실행)
      }
    }, 2000);
  };

  // 순서 분석 실행
  const handleOrderAnalysis = async () => {
    try {
      setOrderAnalysisLoading(true);
      showToast('순서 분석을 시작합니다...', 'info');

      const result = await getOrderAnalysis(parseInt(campaignId));
      setOrderAnalysis(result);
      showToast('순서 분석이 완료되었습니다!', 'success');
    } catch (error) {
      showToast('순서 분석 중 오류가 발생했습니다.', 'error');
      console.error('Order analysis error:', error);
    } finally {
      setOrderAnalysisLoading(false);
    }
  };

  // 순서 위반 조회
  const handleOrderViolations = async () => {
    try {
      setOrderViolationsLoading(true);
      showToast('순서 위반 데이터를 조회합니다...', 'info');

      const result = await getOrderViolations(parseInt(campaignId), 50); // 최대 50개까지
      console.log('📊 순서 위반 API 응답:', result); // 디버깅용 로그

      setOrderViolations(result);

      const totalCount = result?.totalViolations ?? 0;
      const displayCount = result?.violations?.length ?? 0;

      if (totalCount === 0) {
        showToast('순서 위반이 발견되지 않았습니다! 완벽한 순서 보장 ✅', 'success');
      } else {
        showToast(`순서 위반 ${totalCount}건이 조회되었습니다 (표시: ${displayCount}건).`, 'success');
      }
    } catch (error) {
      showToast('순서 위반 조회 중 오류가 발생했습니다.', 'error');
      console.error('❌ 순서 위반 조회 오류:', error);
    } finally {
      setOrderViolationsLoading(false);
    }
  };

  // 비교 결과에 추가
  const addToComparison = () => {
    if (!kafkaResult || !orderAnalysis) {
      showToast('Kafka 테스트 결과와 순서 분석 결과가 모두 필요합니다.', 'warning');
      return;
    }

    // 순서 분석 결과에서 실제 파티션 수 추출
    const actualPartitions = orderAnalysis.partitionDistribution.length;

    const newResult: PartitionTestResult = {
      partitions: actualPartitions,
      kafkaResult,
      orderAnalysis,
    };

    // 중복 체크 (같은 파티션 개수가 이미 있으면 덮어쓰기)
    setComparisonResults((prev) => {
      const filtered = prev.filter((r) => r.partitions !== actualPartitions);
      return [...filtered, newResult].sort((a, b) => a.partitions - b.partitions);
    });

    showToast(`파티션 ${actualPartitions}개 결과를 비교 목록에 추가했습니다.`, 'success');
  };

  // 비교 결과 초기화
  const clearComparison = () => {
    setComparisonResults([]);
    showToast('비교 목록을 초기화했습니다.', 'info');
  };

  // 비교 차트 데이터
  const comparisonData = kafkaResult?.metrics && syncResult?.metrics ? [
    { name: 'P50', Kafka: kafkaResult.metrics.p50, 동기: syncResult.metrics.p50 },
    { name: 'P95', Kafka: kafkaResult.metrics.p95, 동기: syncResult.metrics.p95 },
    { name: 'P99', Kafka: kafkaResult.metrics.p99, 동기: syncResult.metrics.p99 },
    { name: '평균', Kafka: kafkaResult.metrics.avg, 동기: syncResult.metrics.avg },
  ] : [];

  const improvement = kafkaResult?.metrics && syncResult?.metrics
    ? (syncResult.metrics.avg / kafkaResult.metrics.avg).toFixed(1)
    : 0;

  return (
    <Container maxWidth="xl">
      <Typography variant="h4" gutterBottom>
        K6 부하 테스트 결과
      </Typography>



      {/* 테스트 실행 */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Grid container spacing={2} alignItems="center">
          <Grid item xs={12} sm={4}>
            <TextField
              fullWidth
              label="캠페인 ID"
              type="number"
              value={campaignId}
              onChange={(e) => setCampaignId(e.target.value)}
              size="small"
            />
          </Grid>
          <Grid item xs={12} sm={4}>
            <TextField
              fullWidth
              label="총 요청 수"
              type="number"
              value={totalRequests}
              onChange={(e) => setTotalRequests(e.target.value)}
              size="small"
              helperText="1000, 10000, 30000, 100000 등"
            />
          </Grid>
          <Grid item xs={12} sm={4}>
            <Box display="flex" gap={1}>
              <Button
                variant="contained"
                startIcon={kafkaLoading ? <CircularProgress size={20} /> : <PlayArrow />}
                onClick={handleKafkaTest}
                disabled={kafkaLoading || syncLoading}
                fullWidth
              >
                Kafka
              </Button>
              <Button
                variant="contained"
                color="secondary"
                startIcon={syncLoading ? <CircularProgress size={20} /> : <PlayArrow />}
                onClick={handleSyncTest}
                disabled={kafkaLoading || syncLoading}
                fullWidth
              >
                동기
              </Button>
            </Box>
          </Grid>

          {/* 실시간 진행률 표시 */}
          {(kafkaLoading || syncLoading) && (
            <Grid item xs={12}>
              <Box sx={{ mt: 2 }}>
                {kafkaLoading && (
                  <Box sx={{ mb: 2 }}>
                    <Box display="flex" alignItems="center" justifyContent="space-between" mb={1}>
                      <Typography variant="body2" color="primary" fontWeight="bold">
                        <Speed fontSize="small" sx={{ verticalAlign: 'middle', mr: 0.5 }} />
                        Kafka 테스트 실행 중...
                      </Typography>
                      <Chip label={`${Math.round(kafkaProgress)}%`} color="primary" size="small" />
                    </Box>
                    <LinearProgress variant="determinate" value={kafkaProgress} />
                  </Box>
                )}
                {syncLoading && (
                  <Box>
                    <Box display="flex" alignItems="center" justifyContent="space-between" mb={1}>
                      <Typography variant="body2" color="secondary" fontWeight="bold">
                        <Timer fontSize="small" sx={{ verticalAlign: 'middle', mr: 0.5 }} />
                        동기 테스트 실행 중...
                      </Typography>
                      <Chip label={`${Math.round(syncProgress)}%`} color="secondary" size="small" />
                    </Box>
                    <LinearProgress variant="determinate" value={syncProgress} color="secondary" />
                  </Box>
                )}
              </Box>
            </Grid>
          )}
        </Grid>
      </Paper>

      {/* 순서 분석 버튼 */}
      <Paper sx={{ p: 3, mb: 3, bgcolor: 'info.light' }}>
        <Grid container spacing={2} alignItems="center">
          <Grid item xs={12} sm={6}>
            <Typography variant="h6" gutterBottom>
              Kafka 순서 분석
            </Typography>
            <Typography variant="body2" color="text.secondary">
              파티션 개수에 따른 메시지 순서 정확도를 분석합니다. (파티션 1개 = ~100%, 파티션 4개+ = ~70%)
            </Typography>
          </Grid>
          <Grid item xs={12} sm={2}>
            <Button
              variant="contained"
              color="info"
              startIcon={orderAnalysisLoading ? <CircularProgress size={20} /> : <Analytics />}
              onClick={handleOrderAnalysis}
              disabled={orderAnalysisLoading}
              fullWidth
            >
              순서 분석
            </Button>
          </Grid>
          <Grid item xs={12} sm={2}>
            <Button
              variant="contained"
              color="error"
              startIcon={orderViolationsLoading ? <CircularProgress size={20} /> : <Analytics />}
              onClick={handleOrderViolations}
              disabled={orderViolationsLoading}
              fullWidth
            >
              위반 조회
            </Button>
          </Grid>
          <Grid item xs={12} sm={2}>
            <Button
              variant="outlined"
              color="secondary"
              onClick={addToComparison}
              disabled={!kafkaResult || !orderAnalysis}
              fullWidth
            >
              비교 추가
            </Button>
          </Grid>
        </Grid>
      </Paper>

      {/* 비교 결과 (둘 다 있을 때만) */}
      {kafkaResult?.metrics && syncResult?.metrics && (
        <>
          <Paper sx={{ p: 3, mb: 3, bgcolor: 'success.light' }}>
            <Typography variant="h5" gutterBottom>
              🚀 Kafka가 {improvement}배 빠름!
            </Typography>
          </Paper>

          <Paper sx={{ p: 3, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              응답 시간 비교 (ms)
            </Typography>
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={comparisonData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="Kafka" fill="#4caf50" />
                <Bar dataKey="동기" fill="#f44336" />
              </BarChart>
            </ResponsiveContainer>
          </Paper>
        </>
      )}

      {/* 개별 결과 표시 */}
      {(kafkaResult?.metrics || syncResult?.metrics) && (
        <Grid container spacing={3}>
          {kafkaResult?.metrics && (
            <Grid item xs={12} md={kafkaResult?.metrics && syncResult?.metrics ? 6 : 12}>
              <Paper sx={{ p: 3 }}>
                <Typography variant="h6" gutterBottom>
                  Kafka 방식 결과
                </Typography>
                <MetricCard result={kafkaResult} />
              </Paper>
            </Grid>
          )}
          {syncResult?.metrics && (
            <Grid item xs={12} md={kafkaResult?.metrics && syncResult?.metrics ? 6 : 12}>
              <Paper sx={{ p: 3 }}>
                <Typography variant="h6" gutterBottom>
                  동기 방식 결과
                </Typography>
                <MetricCard result={syncResult} />
              </Paper>
            </Grid>
          )}
        </Grid>
      )}

      {/* 순서 분석 결과 */}
      {orderAnalysis && (
        <>
          <Divider sx={{ my: 4 }} />
          <Typography variant="h5" gutterBottom sx={{ mt: 4, mb: 3 }}>
            📊 Kafka 순서 분석 결과
          </Typography>

          {/* 순서 분석 요약 */}
          <Grid container spacing={3} sx={{ mb: 3 }}>
            <Grid item xs={12} md={4}>
              <Card sx={{ bgcolor: 'primary.light' }}>
                <CardContent>
                  <Typography variant="h6" color="primary.dark">총 레코드 수</Typography>
                  <Typography variant="h4">{orderAnalysis.summary.totalRecords.toLocaleString()}</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={4}>
              <Card sx={{ bgcolor: 'warning.light' }}>
                <CardContent>
                  <Typography variant="h6" color="warning.dark">순서 불일치</Typography>
                  <Typography variant="h4">{orderAnalysis.summary.orderMismatches.toLocaleString()}</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={4}>
              <Card sx={{ bgcolor: orderAnalysis.summary.orderAccuracy.includes('100') ? 'success.light' : 'info.light' }}>
                <CardContent>
                  <Typography variant="h6" color={orderAnalysis.summary.orderAccuracy.includes('100') ? 'success.dark' : 'info.dark'}>
                    순서 일치율
                  </Typography>
                  <Typography variant="h4">{orderAnalysis.summary.orderAccuracy}</Typography>
                </CardContent>
              </Card>
            </Grid>
          </Grid>

          {/* 파티션별 분포 */}
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 3 }}>
                <Typography variant="h6" gutterBottom>
                  파티션별 메시지 분포
                </Typography>
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={orderAnalysis.partitionDistribution}
                      dataKey="count"
                      nameKey="partition"
                      cx="50%"
                      cy="50%"
                      outerRadius={100}
                      label={(entry) => `P${entry.partition}: ${entry.count}`}
                    >
                      {orderAnalysis.partitionDistribution.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </Paper>
            </Grid>

            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 3 }}>
                <Typography variant="h6" gutterBottom>
                  파티션별 메시지 수
                </Typography>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={orderAnalysis.partitionDistribution}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="partition" label={{ value: '파티션', position: 'insideBottom', offset: -5 }} />
                    <YAxis label={{ value: '메시지 수', angle: -90, position: 'insideLeft' }} />
                    <Tooltip />
                    <Bar dataKey="count" fill="#8884d8" />
                  </BarChart>
                </ResponsiveContainer>
              </Paper>
            </Grid>
          </Grid>

          {/* 분석 상세 테이블 */}
          <Paper sx={{ p: 3, mt: 3 }}>
            <Typography variant="h6" gutterBottom>
              파티션 분포 상세
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell><strong>파티션 번호</strong></TableCell>
                    <TableCell align="right"><strong>메시지 수</strong></TableCell>
                    <TableCell align="right"><strong>비율</strong></TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {orderAnalysis.partitionDistribution.map((item) => {
                    const percentage = ((item.count / orderAnalysis.summary.totalRecords) * 100).toFixed(2);
                    return (
                      <TableRow key={item.partition}>
                        <TableCell>파티션 {item.partition}</TableCell>
                        <TableCell align="right">{item.count.toLocaleString()}</TableCell>
                        <TableCell align="right">{percentage}%</TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>

          {/* 분석 결과 해석 */}
          <Alert severity={orderAnalysis.summary.orderAccuracy.includes('100') ? 'success' : 'warning'} sx={{ mt: 3 }}>
            <Typography variant="body1" gutterBottom>
              <strong>분석 결과 해석:</strong>
            </Typography>
            <Typography variant="body2">
              - <strong>순서 일치율:</strong> {orderAnalysis.summary.orderAccuracy}
              {orderAnalysis.summary.orderAccuracy.includes('100')
                ? ' (파티션이 1개일 경우 순서가 완벽히 보장됩니다)'
                : ' (파티션이 여러 개일 경우 메시지 순서가 섞일 수 있습니다)'}
            </Typography>
            <Typography variant="body2">
              - <strong>순서 불일치:</strong> {orderAnalysis.summary.orderMismatches}건
              (Kafka offset 순서와 실제 처리 순서가 다른 경우)
            </Typography>
            <Typography variant="body2">
              - <strong>파티션 개수:</strong> {orderAnalysis.partitionDistribution.length}개
              {orderAnalysis.partitionDistribution.length === 1
                ? ' (1개 파티션 = 순서 보장 ✅)'
                : ` (${orderAnalysis.partitionDistribution.length}개 파티션 = 속도 향상, 순서 섞임 가능 ⚠️)`}
            </Typography>
            <Typography variant="body2" sx={{ mt: 1, fontWeight: 'bold' }}>
              💡 결론: 파티션을 늘리면 처리 속도는 빨라지지만, 메시지 순서는 섞일 수 있습니다.
              재고 정확성은 Atomic UPDATE로 보장됩니다!
            </Typography>
          </Alert>
        </>
      )}

      {/* 순서 위반 상세 정보 */}
      {orderViolations && orderViolations.totalViolations !== undefined && (
        <>
          <Divider sx={{ my: 4 }} />
          <Typography variant="h5" gutterBottom sx={{ mt: 4, mb: 3 }}>
            ⚠️ 순서 위반 상세 정보
          </Typography>

          {/* 위반 요약 */}
          <Grid container spacing={3} sx={{ mb: 3 }}>
            <Grid item xs={12} md={4}>
              <Card sx={{ bgcolor: 'error.light' }}>
                <CardContent>
                  <Typography variant="h6" color="error.dark">총 위반 건수</Typography>
                  <Typography variant="h4">{(orderViolations.totalViolations || 0).toLocaleString()}건</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={4}>
              <Card sx={{ bgcolor: 'info.light' }}>
                <CardContent>
                  <Typography variant="h6" color="info.dark">조회 시간</Typography>
                  <Typography variant="h4">{(orderViolations.queryTimeMs || 0).toFixed(2)}ms</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={4}>
              <Card sx={{ bgcolor: 'warning.light' }}>
                <CardContent>
                  <Typography variant="h6" color="warning.dark">표시 건수</Typography>
                  <Typography variant="h4">{(orderViolations.violations?.length || 0)}건</Typography>
                </CardContent>
              </Card>
            </Grid>
          </Grid>

          {/* 위반 상세 테이블 */}
          {orderViolations.violations && orderViolations.violations.length > 0 ? (
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                순서 위반 상세 내역
              </Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell><strong>현재 메시지</strong></TableCell>
                      <TableCell><strong>다음 메시지</strong></TableCell>
                      <TableCell><strong>설명</strong></TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {orderViolations.violations.map((violation, index) => (
                      <TableRow key={index} sx={{ bgcolor: index % 2 === 0 ? 'grey.50' : 'white' }}>
                        <TableCell>
                          <Box sx={{ p: 1, border: '1px solid', borderColor: 'error.main', borderRadius: 1, bgcolor: 'error.light' }}>
                            <Typography variant="body2" fontWeight="bold">
                              User ID: {violation.current.userId}
                            </Typography>
                            <Typography variant="caption" display="block">
                              처리 순서: {violation.current.processingSeq}
                            </Typography>
                            <Typography variant="caption" display="block">
                              파티션: {violation.current.partition} | Offset: {violation.current.offset}
                            </Typography>
                            <Typography variant="caption" display="block" color="text.secondary">
                              {new Date(violation.current.timestamp).toLocaleString('ko-KR')}
                            </Typography>
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Box sx={{ p: 1, border: '1px solid', borderColor: 'warning.main', borderRadius: 1, bgcolor: 'warning.light' }}>
                            <Typography variant="body2" fontWeight="bold">
                              User ID: {violation.next.userId}
                            </Typography>
                            <Typography variant="caption" display="block">
                              처리 순서: {violation.next.processingSeq}
                            </Typography>
                            <Typography variant="caption" display="block">
                              파티션: {violation.next.partition} | Offset: {violation.next.offset}
                            </Typography>
                            <Typography variant="caption" display="block" color="text.secondary">
                              {new Date(violation.next.timestamp).toLocaleString('ko-KR')}
                            </Typography>
                          </Box>
                        </TableCell>
                        <TableCell>
                          <Typography variant="body2" color="error">
                            {violation.explanation}
                          </Typography>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          ) : (
            <Alert severity="success" sx={{ mt: 2 }}>
              <Typography variant="body1">
                순서 위반이 발견되지 않았습니다! 메시지가 올바른 순서로 처리되었습니다.
              </Typography>
            </Alert>
          )}

          {/* 위반 결과 해석 */}
          <Alert severity="info" sx={{ mt: 3 }}>
            <Typography variant="body1" gutterBottom>
              <strong>순서 위반 정보:</strong>
            </Typography>
            <Typography variant="body2">
              - <strong>순서 위반:</strong> Kafka에서 메시지가 전송된 순서와 실제 DB에 처리된 순서가 다른 경우를 의미합니다.
            </Typography>
            <Typography variant="body2">
              - <strong>발생 원인:</strong> 여러 파티션을 사용할 경우, 각 파티션별로 병렬 처리되기 때문에 전체적인 순서가 섞일 수 있습니다.
            </Typography>
            <Typography variant="body2">
              - <strong>영향:</strong> 순서 위반이 있어도 재고 정확성은 Atomic UPDATE로 보장됩니다. 다만 사용자별 처리 순서가 보장되지 않을 수 있습니다.
            </Typography>
            <Typography variant="body2" sx={{ mt: 1, fontWeight: 'bold' }}>
              💡 참고: 파티션이 1개일 경우에는 순서 위반이 발생하지 않습니다.
            </Typography>
          </Alert>
        </>
      )}

      {/* 파티션 비교 결과 */}
      {comparisonResults.length > 0 && (
        <>
          <Divider sx={{ my: 4 }} />
          <Box display="flex" justifyContent="space-between" alignItems="center" sx={{ mb: 3 }}>
            <Typography variant="h5" gutterBottom>
              🔬 파티션 개수별 비교 분석
            </Typography>
            <Button variant="outlined" color="error" onClick={clearComparison}>
              비교 목록 초기화
            </Button>
          </Box>

          {/* 비교 테이블 */}
          <Paper sx={{ p: 3, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              파티션 개수별 성능 비교
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell><strong>파티션 개수</strong></TableCell>
                    <TableCell align="right"><strong>처리 속도 (평균)</strong></TableCell>
                    <TableCell align="right"><strong>순서 일치율</strong></TableCell>
                    <TableCell align="right"><strong>순서 불일치</strong></TableCell>
                    <TableCell align="right"><strong>총 처리량 (TPS)</strong></TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {comparisonResults.map((result) => (
                    <TableRow key={result.partitions}>
                      <TableCell>
                        <Chip
                          label={`${result.partitions}개`}
                          color={result.partitions === 1 ? 'success' : 'primary'}
                          size="small"
                        />
                      </TableCell>
                      <TableCell align="right">
                        {result.kafkaResult?.metrics?.avg.toFixed(2)}ms
                      </TableCell>
                      <TableCell align="right">
                        <Chip
                          label={result.orderAnalysis?.summary.orderAccuracy || 'N/A'}
                          color={result.orderAnalysis?.summary.orderAccuracy.includes('100') ? 'success' : 'warning'}
                          size="small"
                        />
                      </TableCell>
                      <TableCell align="right">
                        {result.orderAnalysis?.summary.orderMismatches.toLocaleString() || 'N/A'}
                      </TableCell>
                      <TableCell align="right">
                        {result.kafkaResult?.metrics?.throughput.toFixed(2)} req/s
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>

          {/* 비교 차트 */}
          <Grid container spacing={3}>
            {/* 처리 속도 비교 */}
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 3 }}>
                <Typography variant="h6" gutterBottom>
                  처리 속도 비교 (낮을수록 좋음)
                </Typography>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart
                    data={comparisonResults.map((r) => ({
                      파티션: `${r.partitions}개`,
                      '평균 응답시간 (ms)': r.kafkaResult?.metrics?.avg || 0,
                    }))}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="파티션" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="평균 응답시간 (ms)" fill="#8884d8" />
                  </BarChart>
                </ResponsiveContainer>
              </Paper>
            </Grid>

            {/* 순서 일치율 비교 */}
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 3 }}>
                <Typography variant="h6" gutterBottom>
                  순서 일치율 비교 (높을수록 좋음)
                </Typography>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart
                    data={comparisonResults.map((r) => ({
                      파티션: `${r.partitions}개`,
                      '순서 일치율 (%)': parseFloat(r.orderAnalysis?.summary.orderAccuracy.replace('%', '') || '0'),
                    }))}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="파티션" />
                    <YAxis domain={[0, 100]} />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="순서 일치율 (%)" fill="#4caf50" />
                  </BarChart>
                </ResponsiveContainer>
              </Paper>
            </Grid>

            {/* 처리량 비교 */}
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 3 }}>
                <Typography variant="h6" gutterBottom>
                  처리량 비교 (높을수록 좋음)
                </Typography>
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart
                    data={comparisonResults.map((r) => ({
                      파티션: `${r.partitions}개`,
                      'TPS (req/s)': r.kafkaResult?.metrics?.throughput || 0,
                    }))}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="파티션" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="TPS (req/s)" stroke="#ff7300" strokeWidth={2} />
                  </LineChart>
                </ResponsiveContainer>
              </Paper>
            </Grid>

            {/* 순서 불일치 비교 */}
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 3 }}>
                <Typography variant="h6" gutterBottom>
                  순서 불일치 비교 (낮을수록 좋음)
                </Typography>
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart
                    data={comparisonResults.map((r) => ({
                      파티션: `${r.partitions}개`,
                      '순서 불일치': r.orderAnalysis?.summary.orderMismatches || 0,
                    }))}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="파티션" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="순서 불일치" stroke="#f44336" strokeWidth={2} />
                  </LineChart>
                </ResponsiveContainer>
              </Paper>
            </Grid>
          </Grid>

          {/* 비교 결과 요약 */}
          <Alert severity="info" sx={{ mt: 3 }}>
            <Typography variant="body1" gutterBottom>
              <strong>📊 비교 분석 요약:</strong>
            </Typography>
            <Typography variant="body2">
              ✅ <strong>처리 속도:</strong> 파티션이 많을수록 병렬 처리로 빨라집니다.
            </Typography>
            <Typography variant="body2">
              ⚠️ <strong>순서 일치율:</strong> 파티션이 1개일 때는 ~100%, 여러 개일 때는 ~70% (메시지 순서가 섞임)
            </Typography>
            <Typography variant="body2">
              💡 <strong>트레이드오프:</strong> 파티션을 늘리면 속도는 빨라지지만 순서 보장은 약해집니다.
            </Typography>
            <Typography variant="body2" sx={{ mt: 1, fontWeight: 'bold' }}>
              ✨ <strong>재고 정확성:</strong> 파티션 개수와 관계없이 Atomic UPDATE로 항상 정확합니다!
            </Typography>
          </Alert>
        </>
      )}
    </Container>
  );
};

// 숫자 애니메이션 컴포넌트
const AnimatedNumber = ({ value, decimals = 2, suffix = '' }: { value: number; decimals?: number; suffix?: string }) => {
  const [displayValue, setDisplayValue] = useState(0);

  useEffect(() => {
    const duration = 1000; // 1초
    const steps = 60;
    const stepValue = value / steps;
    let current = 0;

    const timer = setInterval(() => {
      current += stepValue;
      if (current >= value) {
        setDisplayValue(value);
        clearInterval(timer);
      } else {
        setDisplayValue(current);
      }
    }, duration / steps);

    return () => clearInterval(timer);
  }, [value]);

  return <span>{displayValue.toFixed(decimals)}{suffix}</span>;
};

// 메트릭 카드 컴포넌트
const MetricCard = ({ result }: { result: LoadTestResult }) => {
  const [show, setShow] = useState(false);

  useEffect(() => {
    setShow(true);
  }, []);

  return (
    <Box sx={{ opacity: show ? 1 : 0, transition: 'opacity 0.5s' }}>
      <Typography variant="body2" color="text.secondary">응답 시간</Typography>
      <Typography>
        평균: <AnimatedNumber value={result.metrics?.avg ?? 0} />ms
      </Typography>
      <Typography>
        P50: <AnimatedNumber value={result.metrics?.p50 ?? 0} />ms
      </Typography>
      <Typography>
        P95: <AnimatedNumber value={result.metrics?.p95 ?? 0} />ms
      </Typography>
      <Typography>
        P99: <AnimatedNumber value={result.metrics?.p99 ?? 0} />ms
      </Typography>
      <Typography>
        최대: <AnimatedNumber value={result.metrics?.max ?? 0} />ms
      </Typography>

      <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>처리량</Typography>
      <Typography>
        총 요청: <AnimatedNumber value={result.metrics?.totalRequests ?? 0} decimals={0} />
      </Typography>
      <Typography>
        TPS: <AnimatedNumber value={result.metrics?.throughput ?? 0} suffix=" req/s" />
      </Typography>

      <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>실패율</Typography>
      <Typography>
        <AnimatedNumber value={(result.metrics?.failureRate ?? 0) * 100} suffix="%" />
      </Typography>
    </Box>
  );
};

export default LoadTestResults;
