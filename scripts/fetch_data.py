#!/usr/bin/env python3
"""
Fetch Flash Express branch data from Feishu and generate data.json
Run by GitHub Actions daily.
"""
import json
import os
import ssl
import urllib.request
from datetime import datetime, timedelta

FEISHU_APP_ID = os.environ['FEISHU_APP_ID']
FEISHU_APP_SECRET = os.environ['FEISHU_APP_SECRET']
SPREADSHEET = 'KyaLs47uThMhWZtdKBsc1BH8n5f'
SHEET_ID = 'LvMcIM'
TOTAL_ROWS = 2820

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def api_get(url, token):
    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
    with urllib.request.urlopen(req, context=ctx) as r:
        return json.loads(r.read())


def get_token():
    url = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
    payload = json.dumps({'app_id': FEISHU_APP_ID, 'app_secret': FEISHU_APP_SECRET}).encode()
    req = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, context=ctx) as r:
        data = json.loads(r.read())
    if data.get('code') != 0:
        raise RuntimeError(f"Token error: {data}")
    return data['tenant_access_token']


def fetch_range(token, start, end):
    url = (f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/'
           f'{SPREADSHEET}/values/{SHEET_ID}%21A{start}%3AAP{end}')
    data = api_get(url, token)
    if data.get('code') != 0:
        raise RuntimeError(f"API error: {data}")
    return data['data']['valueRange'].get('values', [])


def excel_to_date(serial):
    if not serial or not isinstance(serial, (int, float)):
        return ''
    try:
        base = datetime(1899, 12, 30)
        return (base + timedelta(days=int(serial))).strftime('%Y-%m-%d')
    except Exception:
        return str(serial)


def get_type_simple(bt):
    if not bt:
        return ''
    bt = str(bt).strip()
    if 'HUB' in bt or 'To HUB' in bt:
        return 'HUB'
    if 'To DC' in bt or (bt.startswith('DC') and 'DP' not in bt):
        return 'DC'
    if 'DP0' in bt or '一键注册' in bt:
        return 'DP0'
    if 'DP12000' in bt:
        return 'DP12000'
    if 'DP 13000' in bt or 'DP13000' in bt:
        return 'DP13000'
    if 'DP 1000' in bt:
        return 'DP1000'
    if 'DP 9999' in bt or 'DP7990' in bt:
        return 'DP9999+'
    if 'KA' in bt:
        return 'KA'
    return bt[:20]


def clean(v):
    return '' if v is None else str(v).strip()


def main():
    print('Getting Feishu token...')
    token = get_token()

    print('Fetching spreadsheet data...')
    all_rows = []
    batch = 500

    for start in range(1, TOTAL_ROWS, batch):
        end = min(start + batch - 1, TOTAL_ROWS)
        rows = fetch_range(token, start, end)
        if start == 1 and rows:
            rows = rows[1:]  # skip header row
        all_rows.extend([r for r in rows if r and len(r) > 2 and r[2]])
        print(f'  Fetched rows {start}-{end}, accumulated: {len(all_rows)}')

    print(f'Total raw rows: {len(all_rows)}')

    # Keep latest record per FH ID
    branch_map = {}
    for row in all_rows:
        fh_id = clean(row[2])
        if not fh_id:
            continue
        date = row[0] if row[0] else 0
        if fh_id not in branch_map or (date and date > branch_map[fh_id][0]):
            branch_map[fh_id] = row

    branches = []
    for row in branch_map.values():
        def g(i, r=row):
            return clean(r[i]) if len(r) > i else ''
        branches.append({
            'date': excel_to_date(row[0] if row else None),
            'am': g(1), 'fh_id': g(2), 'name': g(3), 'pct': g(4),
            'type': g(5), 'type_simple': get_type_simple(g(5)),
            'province': g(6),
            'create_acc': excel_to_date(row[7] if len(row) > 7 else None),
            'bd': g(8), 'bd_id': g(9), 'region': g(10),
            'network_type': g(11), 'contact_status': g(14),
            'another_branch': g(15), 'single_system': g(16),
            'competitor': g(17), 'partner_brand': g(18),
            'shop_design': g(20), 'rent_or_own': g(21),
            'rent_amount': g(22), 'has_staff': g(23), 'staff_cost': g(24),
            'other_biz': g(25), 'coupon_type': g(26),
            'gps_correct': g(29), 'address': g(30), 'gps': g(31),
            'location': g(34), 'cooperation': g(37),
            'attention_level': g(38), 'suggestions': g(39),
            'status': g(40), 'termination': g(41),
        })

    output = {
        'updated_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'total': len(branches),
        'branches': branches,
    }

    out_path = os.path.join(os.path.dirname(__file__), '..', 'data.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    print(f'Done: {len(branches)} branches written to data.json')


if __name__ == '__main__':
    main()
