"""Offline checks only; do not start Docker or access VDP shared services."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'benchmarks' / 'read'))
import trino_read_benchmark as trino


class TestRepoLayout(unittest.TestCase):
    def test_paths_and_public_secrets(self):
        for path in [
            'compose.yaml', 'compose/ozone.yml', 'compose/kafka.yml',
            'platform/flink/Dockerfile',
            'platform/flink/config/init.sql.template',
            'platform/flink/jobs/bronze_ingestion/ami_meter.sql',
            'platform/iceberg/config/create-warehouse.json.template',
            'platform/trino/catalog/lakehouse.properties.template',
            'benchmarks/ingestion/benchmark_ami_ramp.py',
            'benchmarks/metadata/benchmark_lakehouse_metadata_growth.py',
            'benchmarks/read/trino_read_benchmark.py',
            'benchmarks/recovery/run-datanode-leader-failure-test.ps1',
        ]:
            self.assertTrue((ROOT / path).is_file(), path)
        self.assertFalse((ROOT / '.env').exists(), '.env must not be packaged')
        self.assertTrue((ROOT / '.env.example').exists())

    def test_compose_paths(self):
        import yaml
        paths = {
            'compose/flink.yml': ['platform/flink/config/init.sql', 'platform/flink/config/smoke.sql', 'platform/flink/jobs'],
            'compose/trino.yml': ['platform/trino/catalog'],
        }
        for file, members in paths.items():
            text=(ROOT / file).read_text()
            yaml.safe_load(text)
            for member in members:
                self.assertIn(member, text)
                self.assertTrue((ROOT / member).exists() or (ROOT / (member + ".template")).exists())
        for name in ['ozone', 'kafka', 'flink', 'iceberg-rest', 'trino', 'simulator']:
            self.assertTrue((ROOT / 'compose' / (name + '.yml')).is_file())


class TestPrivateConfigRenderer(unittest.TestCase):
    def test_templates_render_with_provided_local_keys_only(self):
        import shutil
        from io import StringIO
        from contextlib import redirect_stdout
        script=ROOT/'scripts'/'render-local-config.py'
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            for rel in ['platform/flink/config/init.sql',
                        'platform/iceberg/config/create-warehouse.json',
                        'platform/trino/catalog/lakehouse.properties']:
                target=root/(rel+'.template')
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT/(rel+'.template'),target)
            (root/'.env').write_text('S3_ACCESS_KEY=DEVACCESS123\nS3_SECRET_KEY=DEVSECRET123456\n')
            proc=subprocess.run([sys.executable,str(script),'--root',str(root)],
                capture_output=True,text=True,check=True)
            for rel in ['platform/flink/config/init.sql',
                        'platform/iceberg/config/create-warehouse.json',
                        'platform/trino/catalog/lakehouse.properties']:
                value=(root/rel).read_text()
                self.assertIn('DEVACCESS123',value)
                self.assertIn('DEVSECRET123456',value)
                self.assertNotIn('{{S3_',value)
                self.assertTrue((root/(rel+'.template')).is_file())


class TestSimulator(unittest.TestCase):
    def test_duplicate_is_exact_same_payload_and_rate_slot(self):
        script=ROOT/'simulator'/'ami'/'ami_simulator.py'
        proc=subprocess.run([sys.executable,str(script), '--mode','custom',
            '--target-rate','100', '--meters','3', '--duplicate-rate','1',
            '--dry-run-events','5'],capture_output=True,text=True,timeout=15,check=True)
        rows=[json.loads(s) for s in proc.stdout.splitlines() if s.startswith('{"key"')]
        self.assertEqual(len(rows),5)
        self.assertFalse(rows[0]['duplicate'])
        self.assertTrue(all(r['duplicate'] for r in rows[1:]))
        self.assertTrue(all(r['value']==rows[0]['value'] for r in rows))
        self.assertTrue(all(r['key']==rows[0]['key'] for r in rows))
        self.assertTrue(all(r['expected_parse_status']=='OK' for r in rows))


class TestTrinoRead(unittest.TestCase):
    def test_sql_is_snapshot_pinned(self):
        args=trino.parse_args(['--snapshot-id','123', '--duration-seconds','1'])
        sql=trino.make_sql(args,102)
        self.assertIn('FOR VERSION AS OF 123',sql)
        self.assertIn('MOD(kafka_offset, 100) = 2',sql)
        self.assertEqual(trino.percentile([100,200,300],0.5),200)

    def test_trino_pagination_parses_end_of_query(self):
        results=[{'id':'query1','nextUri':'http://localhost:8080/v1/statement/query1/1'},
                 {'id':'query1','stats':{'elapsedTimeMillis':18}}]
        with patch.object(trino,'get_json',side_effect=results) as mock_get:
            val=trino.query_trino('http://localhost:8080','SELECT 1','user',timeout=1)
        self.assertEqual(mock_get.call_count,2)
        self.assertEqual(val['query_id'],'query1')
        self.assertEqual(val['stats']['elapsedTimeMillis'],18)

    def test_trino_blocks_cross_host_next_uri(self):
        with patch.object(trino,'get_json',return_value={
            'id':'q','nextUri':'http://other-host:8080/v1/statement/q/1'}):
            with self.assertRaisesRegex(RuntimeError,'different host'):
                trino.query_trino('http://localhost:8080','SELECT 1','user',1)

    def test_runner_writes_structured_outputs(self):
        args=trino.parse_args(['--snapshot-id','42','--duration-seconds','0.01','--warmup-queries','0'])
        def fake_client(*_):
            return {'query_id':'fake','stats':{'elapsedTimeMillis':1}}
        # Use deterministic elapsed time so test runs exactly one short query.
        with tempfile.TemporaryDirectory() as temp:
            args.output_root=temp
            with patch.object(trino.time,'monotonic',side_effect=[0,0,0,0.02,0.03,0.04]):
                rc=trino.run(args,client=fake_client)
            self.assertEqual(rc,0)
            folders=list(Path(temp).glob('*'))
            self.assertEqual(len(folders),1)
            for name in ['queries.csv','summary.json','benchmark_manifest.json']:
                self.assertTrue((folders[0]/name).is_file())
            summary=json.loads((folders[0]/'summary.json').read_text())
            self.assertEqual(summary['failed_queries'],0)
            self.assertEqual(summary['query_count'],1)


if __name__ == '__main__':
    unittest.main()
