#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess, time, os, json, shutil
import matplotlib
matplotlib.use('Agg')
import argparse, requests, matplotlib.pyplot as plt
import random
import numpy as np

def start_process(path, args):
    """启动子进程"""
    dev_null = open(os.devnull, 'w')
    return subprocess.Popen([path] + args, stdout=dev_null, stderr=dev_null, text=True)

def get_hnsw_mem(port=8080):
    """调用 /mem 接口获取 hnsw_service 的 RSS"""
    try:
        resp = requests.get(f"http://127.0.0.1:{port}/mem", timeout=20)
        if resp.ok:
            return resp.json().get("rss_kb", 0.0) / 1024.0
    except Exception as e:
        print("Failed to get /mem:", e)
    return 0.0

def clean_data(dbpath='./rocksdb_data'):
    """删除旧的数据"""
    if os.path.exists(dbpath):
        shutil.rmtree(dbpath)
    if os.path.exists('./hnsw_graph.bin'):
        os.remove('./hnsw_graph.bin')
    if os.path.exists('./hnsw_graph.bin.adj'):
        os.remove('./hnsw_graph.bin.adj')

def get_ground_truth_numpy(query_vector, all_vectors, k=10):
    """使用numpy向量化计算真实的最邻近结果"""
    if not all_vectors:
        return []
    
    # 将向量转换为numpy数组
    vec_ids = list(all_vectors.keys())
    vectors_array = np.array([all_vectors[vid] for vid in vec_ids])
    query_array = np.array(query_vector)
    
    # 批量计算L2距离（向量化操作）
    # (query - vectors)^2 然后沿axis=1求和
    distances = np.sum((query_array - vectors_array) ** 2, axis=1)
    
    # 获取前k个最小距离的索引
    if k < len(distances):
        topk_indices = np.argpartition(distances, k)[:k]
        # 对前k个结果按距离排序
        topk_indices = topk_indices[np.argsort(distances[topk_indices])]
    else:
        topk_indices = np.argsort(distances)
    
    # 返回对应的向量ID
    return [vec_ids[i] for i in topk_indices]

def calculate_recall(hnsw_results, ground_truth):
    """计算召回率"""
    hnsw_ids = set([result[0] for result in hnsw_results])
    gt_ids = set(ground_truth)
    
    intersection = hnsw_ids.intersection(gt_ids)
    recall = len(intersection) / len(gt_ids) if gt_ids else 0.0
    return recall

def fetch_all_vectors_numpy(storage_port=8081, max_vectors=100000, hnsw_process=None):
    """从storage_service获取所有向量（numpy优化版本）"""
    vectors = {}
    successful_count = 0
    failed_count = 0
    
    print(f"[VECTOR FETCH] Starting to fetch vectors from storage...")
    
    # 批量获取向量，减少HTTP请求开销
    batch_size = 100
    for start_id in range(0, max_vectors, batch_size):
        if hnsw_process.poll() is not None:
            break
            
        end_id = min(start_id + batch_size, max_vectors)
        batch_vectors = {}
        
        for vec_id in range(start_id, end_id):
            try:
                resp = requests.get(f"http://127.0.0.1:{storage_port}/vec/get?id={vec_id}", timeout=2)
                if resp.ok:
                    data = resp.json()
                    batch_vectors[vec_id] = data['values']
                    successful_count += 1
                else:
                    failed_count += 1
                    if failed_count > 100:  # 如果连续失败太多，提前结束
                        break
            except:
                failed_count += 1
                if failed_count > 100:
                    break
        
        vectors.update(batch_vectors)
        
        if successful_count % 10000 == 0:
            print(f"[VECTOR FETCH] Fetched {successful_count} vectors...")
        
        # 如果这一批一个都没成功，可能已经到末尾了
        if not batch_vectors and failed_count > 10:
            break
    
    print(f"[VECTOR FETCH] Completed: {successful_count} successful, {failed_count} failed")
    return vectors


def run_experiment(sizes, dim=128, dbpath='./rocksdb_data', opt_mode=False, dpi=200, n_search=20, M=16, ef_construction=200, measure_recall=False, measure_memory=True):
    results = []
    mode = "optimized" if opt_mode else "baseline"
    os.makedirs('res', exist_ok=True)
    print(f"\n==== Running {mode} mode ====\n")

    for N in sizes:
        print(f"[INFO] Building index N={N}  (mode={mode})")

        # 清理旧数据
        clean_data(dbpath)
        time.sleep(0.5)

        # 构建索引
        proc = subprocess.run(['./index_builder', str(N), str(dim), dbpath, './hnsw_graph.bin',
                              str(M), str(ef_construction)], text=True)
        if proc.returncode != 0:
            print("index_builder failed with return code", proc.returncode)
            return

        # 启动 storage_service
        storage = start_process('./storage_service', [dbpath, '8081'])
        time.sleep(1.0)

        graph_path = './hnsw_graph.bin'
        hnsw = start_process(
            './hnsw_service',
            ['--graph', graph_path,
            '--storage', 'http://127.0.0.1:8081',
            '--port', '8080',
            '--ef', '200',
            '--optimized', str(int(opt_mode)),
            '--dim', str(dim)]
        )
        time.sleep(1.5)

        # 确保/mem接口正常
        for _ in range(10):
            if get_hnsw_mem() > 0:
                break
            time.sleep(0.5)

        # 召回率测量
        recall_results = []
        avg_recall = 0.0
        
        if measure_recall:
            print(f"[RECALL] Measuring recall for {mode} mode, N={N}")
            
            # 获取所有向量用于计算真实结果
            all_vectors = fetch_all_vectors_numpy(8081, N, hnsw_process=hnsw)
            
            if len(all_vectors) >= 20:  # 确保有足够的数据进行测试
                # 生成测试查询（从数据库中随机选择）
                test_ids = random.sample(list(all_vectors.keys()), min(10, len(all_vectors) // 2))
                
                print(f"[RECALL] Testing with {len(test_ids)} queries...")
                
                for i, query_id in enumerate(test_ids):
                    if hnsw.poll() is not None:
                        print(f"hnsw_service 在第 {i+1} 次召回测试前已崩溃")
                        break
                    
                    try:
                        query_vector = all_vectors[query_id]
                        
                        # 计算真实结果（排除查询向量自身）
                        vectors_for_gt = {vid: vec for vid, vec in all_vectors.items() if vid != query_id}
                        ground_truth = get_ground_truth_numpy(query_vector, vectors_for_gt, k=20)
                        
                        if not ground_truth:
                            print(f"[RECALL] Warning: No ground truth for query {i+1}")
                            recall_results.append(0.0)
                            continue
                        
                        # HNSW搜索
                        query = {
                            "query": query_vector,
                            "k": 20, 
                            'ef': 200
                        }
                        
                        resp = requests.post("http://127.0.0.1:8080/search", json=query, timeout=120)
                        if resp.ok:
                            hnsw_results = resp.json().get('results', [])
                            # 转换为(id, distance)格式
                            hnsw_pairs = [(result['id'], result['distance']) for result in hnsw_results]
                            
                            # 计算召回率
                            recall = calculate_recall(hnsw_pairs, ground_truth)
                            recall_results.append(recall)
                            
                            print(f"[RECALL] Query {i+1}/{len(test_ids)}: Recall = {recall:.4f}")
                            
                            # 调试信息：显示部分结果对比
                              # 只在第一次查询时显示详细对比
                            if i==0:
                                print(f"[DEBUG] Ground truth IDs: {ground_truth}")
                                print(f"[DEBUG] HNSW result IDs: {[r[0] for r in hnsw_pairs]}")
                                print(f"[DEBUG] Intersection: {set(ground_truth) & set([r[0] for r in hnsw_pairs])}")
                                    
                        else:
                            print(f"[RECALL] Query {i+1} failed: HTTP {resp.status_code}")
                            recall_results.append(0.0)
                            
                    except Exception as e:
                        print(f"[RECALL] Query {i+1} failed: {e}")
                        recall_results.append(0.0)
                    
                    time.sleep(0.2)
                
                # 计算平均召回率
                if recall_results:
                    avg_recall = np.mean(recall_results)
                    std_recall = np.std(recall_results)
                    print(f"[RECALL] N={N} {mode}: Average Recall = {avg_recall:.4f} ± {std_recall:.4f}")
                else:
                    avg_recall = 0.0
                    print(f"[RECALL] No valid recall results")
            else:
                print(f"[RECALL] Warning: Insufficient vectors for recall test ({len(all_vectors)} < 20)")
                avg_recall = 0.0

        if measure_memory:
            # 内存测量
            print(f"[INFO] Starting {n_search} search requests to measure memory...")
            mem_trace = []
            
            # 使用随机查询进行内存测量
            for i in range(n_search):
                if hnsw.poll() is not None:
                    print(f"hnsw_service 在第 {i+1} 次搜索前已崩溃")
                    break

                try:
                    # 生成随机查询向量
                    query = {
                        "query": [random.uniform(-1.0, 1.0) for _ in range(dim)], 
                        "k": 10, 
                        'ef': 200
                    }

                    # 发出/search请求
                    resp = requests.post("http://127.0.0.1:8080/search", json=query, timeout=120)
                    if resp.ok:
                        res = resp.json().get('results', [])
                        print(f"Search {i+1}/{n_search}: got {len(res)} results")
                    else:
                        print(f"Search {i+1}/{n_search}: HTTP {resp.status_code}")
                except requests.exceptions.Timeout:
                    print(f"Search {i+1}/{n_search}: 超时，服务可能已崩溃")
                    if hnsw.poll() is not None:
                        print("hnsw_service 已崩溃")
                        break
                except Exception as e:
                    print(f"Search {i+1} failed:", e)

                # 记录当前RSS
                try:
                    if hnsw.poll() is None:
                        rss = get_hnsw_mem()
                        mem_trace.append(rss)
                    else:
                        mem_trace.append(0)
                except Exception as e:
                    print(f"Failed to get memory usage:", e)

                time.sleep(0.2)

            # 计算统计数据
            avg_mem = sum(mem_trace) / len(mem_trace) if mem_trace else 0.0
            peak_mem = max(mem_trace) if mem_trace else 0.0
        else:
            print(f"[INFO] Skipping memory measurement as requested")
            avg_mem = 0.0
            peak_mem = 0.0
            mem_trace = []
        
        # 保存结果
        results.append({
            'N': N, 
            'avg_rss': avg_mem, 
            'peak_rss': peak_mem, 
            'avg_recall': avg_recall,
            'recall_results': recall_results if measure_recall else [],
            'memory_measured': measure_memory
        })
        if measure_memory:
            print(f"[RESULT] N={N} avg={avg_mem:.1f}MB peak={peak_mem:.1f}MB recall={avg_recall:.4f}")
        else:
            print(f"[RESULT] N={N} recall={avg_recall:.4f} (memory measurement skipped)")
        # 关闭服务
        hnsw.terminate()
        storage.terminate()
        hnsw.wait()
        storage.wait()
        time.sleep(1.0)

    # 汇总绘图
    Ns = [r['N'] for r in results]
    if measure_memory:
        avg_vals = [r['avg_rss'] for r in results]
        peak_vals = [r['peak_rss'] for r in results]

        plt.figure(figsize=(6,4))
        plt.plot(Ns, avg_vals, marker='o', label='Average RSS')
        plt.plot(Ns, peak_vals, marker='x', label='Peak RSS')
        plt.xlabel('N')
        plt.ylabel('RSS (MB)')
        plt.title(f'HNSW {mode} Memory Usage')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f'res/memory_usage_{mode}.png', dpi=dpi)
        plt.close()

    # 如果测量了召回率，绘制召回率图表
    if measure_recall:
        recall_vals = [r['avg_recall'] for r in results]
        plt.figure(figsize=(6,4))
        plt.plot(Ns, recall_vals, marker='s', label='Average Recall', color='red')
        plt.xlabel('N')
        plt.ylabel('Recall')
        plt.title(f'HNSW {mode} Recall Rate')
        plt.ylim(0, 1.0)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f'res/recall_rate_{mode}.png', dpi=dpi)
        plt.close()

    # 保存结果
    with open(f'res/results_{mode}.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n{mode} experiment finished. Results saved to res/results_{mode}.json")

    return results


def calculate_memory_reduction(baseline_results, optimized_results, dpi=200):
    """计算内存下降比例和召回率对比"""
    if not baseline_results or not optimized_results:
        print("没有可对比的数据")
        return
        
    baseline_dict = {r['N']: r for r in baseline_results}
    optimized_dict = {r['N']: r for r in optimized_results}
    
    # 确保N值集合一致
    common_Ns = sorted(set(baseline_dict.keys()) & set(optimized_dict.keys()))
    if not common_Ns:
        print("没有可对比的N值数据")
        return
    
    # 计算召回率差异（无论是否测量内存都计算）
    recall_differences = []
    for N in common_Ns:
        baseline = baseline_dict[N]
        optimized = optimized_dict[N]
        
        # 召回率差异（优化模式召回率 - 基线召回率）
        recall_diff = optimized.get('avg_recall', 0) - baseline.get('avg_recall', 0)
        recall_differences.append(recall_diff)
        
        print(f"[COMPARISON] N={N} 召回率差异: {recall_diff:+.4f}")

    print(f"[DEBUG] recall_differences: {recall_differences}")
    print(f"[DEBUG] any(recall_differences): {any(recall_differences)}")
    # 如果测量了召回率，绘制召回率对比图
    if any(recall_differences):
        plt.figure(figsize=(6, 4))
        plt.plot(common_Ns, recall_differences, marker='s', label='Recall Difference (Opt - Base)', color='green')
        plt.xlabel('Vector Count (N)')
        plt.ylabel('Recall Difference')
        plt.title('HNSW Recall Comparison (Optimized - Baseline)')
        plt.axhline(y=0, color='r', linestyle='--', alpha=0.5)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig('res/recall_comparison.png', dpi=dpi)
        plt.close()
        print("[COMPARISON] 召回率对比图已保存到 res/recall_comparison.png")
    
    # 检查是否测量了内存，如果测量了则计算内存下降比例
    memory_measured = (
        baseline_results[0].get('memory_measured', True) and 
        optimized_results[0].get('memory_measured', True)
    )
    
    if not memory_measured:
        print("[COMPARISON] 内存测量被跳过，仅显示召回率对比")
        # 保存召回率对比结果
        recall_comparison_results = [{
            'N': N,
            'recall_difference': recall_diff
        } for N, recall_diff in zip(common_Ns, recall_differences)]
        
        with open('res/recall_comparison.json', 'w') as f:
            json.dump(recall_comparison_results, f, indent=2)
        return
    
    # 计算内存下降比例（只有测量了内存时才执行）
    print("[COMPARISON] 计算内存下降比例...")
    avg_reductions = []
    peak_reductions = []
    
    for N in common_Ns:
        baseline = baseline_dict[N]
        optimized = optimized_dict[N]
        
        # 内存下降比例计算
        if baseline['avg_rss'] == 0:
            avg_reduction = 0.0
        else:
            avg_reduction = (baseline['avg_rss'] - optimized['avg_rss']) / baseline['avg_rss'] * 100
        
        if baseline['peak_rss'] == 0:
            peak_reduction = 0.0
        else:
            peak_reduction = (baseline['peak_rss'] - optimized['peak_rss']) / baseline['peak_rss'] * 100
        
        avg_reductions.append(avg_reduction)
        peak_reductions.append(peak_reduction)
        
        print(f"[REDUCTION] N={N} 平均内存下降: {avg_reduction:.1f}%  峰值内存下降: {peak_reduction:.1f}%  召回率差异: {recall_differences[common_Ns.index(N)]:+.4f}")
    
    # 绘制内存下降比例图
    plt.figure(figsize=(8, 5))
    plt.plot(common_Ns, avg_reductions, marker='o', label='Average Memory Reduction')
    plt.plot(common_Ns, peak_reductions, marker='x', label='Peak Memory Reduction')
    plt.xlabel('Vector Count (N)')
    plt.ylabel('Memory Reduction (%)')
    plt.title('HNSW Optimized Mode Memory Reduction')
    plt.ylim(0, 100)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('res/memory_reduction.png', dpi=dpi)
    plt.close()
    
    # 保存完整对比结果
    comparison_results = [{
        'N': N,
        'avg_reduction_pct': avg,
        'peak_reduction_pct': peak,
        'recall_difference': recall_diff
    } for N, avg, peak, recall_diff in zip(common_Ns, avg_reductions, peak_reductions, recall_differences)]
    
    with open('res/comparison_results.json', 'w') as f:
        json.dump(comparison_results, f, indent=2)
    print(f"\n对比分析完成，结果保存到 res/comparison_results.json")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sizes', nargs='+', type=int, default=[10000, 50000, 100000])
    parser.add_argument('--dim', type=int, default=128)
    parser.add_argument('--opt', action='store_true', help='also run optimized mode test')
    parser.add_argument('--dpi', type=int, default=200)
    parser.add_argument('--n_search', type=int, default=10, help='number of /search requests per test')
    parser.add_argument('--M', type=int, default=16, help='max neighbors per node')
    parser.add_argument('--ef_construction', type=int, default=200, help='ef_construction parameter')
    parser.add_argument('--recall', action='store_true', help='measure recall rate')
    # parser.add_argument('--mem', action='store_true', default=True, help='measure memory usage (default: True)')
    parser.add_argument('--no-mem', action='store_true', help='skip memory measurement')
    args = parser.parse_args()

    # 非优化模式
    baseline_results = run_experiment(args.sizes, args.dim, opt_mode=False, dpi=args.dpi, n_search=args.n_search, M=args.M, ef_construction=args.ef_construction, measure_recall=args.recall, measure_memory=not args.no_mem)
    # 优化模式
    if args.opt:
       optimized_results = run_experiment(args.sizes, args.dim, opt_mode=True, dpi=args.dpi, n_search=args.n_search, M=args.M, ef_construction=args.ef_construction, measure_recall=args.recall, measure_memory=not args.no_mem)
       calculate_memory_reduction(baseline_results, optimized_results, args.dpi)




    