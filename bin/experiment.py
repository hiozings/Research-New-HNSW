import subprocess, time, os, json, shutil
import argparse, requests
import matplotlib.pyplot as plt

def start_process(path, args):
    return subprocess.Popen([path] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def get_hnsw_mem(port=8080):
    try:
        resp = requests.get(f"http://127.0.0.1:{port}/mem", timeout=3)
        if resp.ok:
            return resp.json().get("rss", 0.0)
    except Exception as e:
        print("Failed to get /mem:", e)
    return 0.0

def run_experiment(sizes, dim=128, dbpath='./rocksdb_data', opt_mode=False, dpi=200):
    results = []
    mode = "optimized" if opt_mode else "baseline"
    print(f"==== Running {mode} mode ====")

    for N in sizes:
        print(f"\n[INFO] Building index N={N}  (mode={mode})")

        # 清理旧数据
        if os.path.exists(dbpath):
            shutil.rmtree(dbpath)
        if os.path.exists('./hnsw_graph.bin'):
            os.remove('./hnsw_graph.bin')
        time.sleep(0.5)

        # 构建索引
        proc = subprocess.run(['./index_builder', str(N), str(dim), dbpath, './hnsw_graph.bin'])
        if proc.returncode != 0:
            print("index_builder failed with return code", proc.returncode)
            return

        # 启动服务
        storage = start_process('./storage_service', [dbpath, '8081'])
        time.sleep(1.0)
        hnsw = start_process(
            './hnsw_service',
            ['--graph', './hnsw_graph.bin',
             '--storage', 'http://127.0.0.1:8081',
             '--port', '8080',
             '--ef', '200',
             '--opt', str(int(opt_mode))]
        )
        time.sleep(1.5)

        # 发送 /search 请求
        try:
            query = {"query": [0.1]*dim, "k": 10}
            resp = requests.post("http://127.0.0.1:8080/search", json=query, timeout=5)
            if resp.ok:
                print(f"/search OK: got {len(resp.json()['results'])} results")
            else:
                print("/search failed", resp.status_code)
        except Exception as e:
            print("search request failed:", e)

        # 获取内存
        mem_hnsw = get_hnsw_mem()
        results.append({'N': N, 'hnsw_rss': mem_hnsw})
        print(f"N={N} HNSW RSS={mem_hnsw:.1f} MB")

        # 停止服务
        hnsw.terminate()
        storage.terminate()
        time.sleep(1.0)

    # 绘图
    Ns = [r['N'] for r in results]
    hvals = [r['hnsw_rss'] for r in results]
    plt.plot(Ns, hvals, marker='o', label=f'HNSW {mode}')
    plt.xlabel('N')
    plt.ylabel('RSS (MB)')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'memory_usage_{mode}.png', dpi=dpi)
    with open(f'results_{mode}.json','w') as f:
        json.dump(results, f, indent=2)
    print(f"✅ {mode} experiment finished. Results saved to results_{mode}.json and memory_usage_{mode}.png")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sizes', nargs='+', type=int, default=[10000,50000,100000])
    parser.add_argument('--dim', type=int, default=128)
    parser.add_argument('--opt', action='store_true', help='enable optimized mode test')
    parser.add_argument('--dpi', type=int, default=200)
    args = parser.parse_args()

    # 非优化模式
    run_experiment(args.sizes, args.dim, opt_mode=False, dpi=args.dpi)
    # 优化模式
    if args.opt:
        run_experiment(args.sizes, args.dim, opt_mode=True, dpi=args.dpi)



    