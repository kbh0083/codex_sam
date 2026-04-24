# SSH 접속 정보

> 민감 정보가 포함된 운영 문서다. 외부 공유를 금지한다.
> last updated: 2026-04-24
> 최신 실행 인계는 [handoff_26042401.md](/Users/bhkim/Documents/codex_prj_sam_asset/세션/handoff_26042401.md), 전체 시작 문서는 [00_시작_안내.md](/Users/bhkim/Documents/codex_prj_sam_asset/readme/00_시작_안내.md)를 우선 본다.

## LLM 터널 정보
- 로컬 : ssh -L 3910:localhost:3950 minisoft@1.241.20.229 -p 2194
- was : ssh -L 3900:localhost:3950 minisoft@1.241.20.229 -p 2194
- 암호 (로컬/was 동일) : Mini1234!
- 현재 extractor 실효 `base_url`은 로컬 `3910`, WAS `3900`이다.
- 현재 extractor 실효 `max_tokens`는 로컬 `16384`, WAS `4090`이다.
- 연결 확인은 아래 순서로 수행한다.
  1. `lsof -nP -iTCP:3910 -sTCP:LISTEN`
  2. 인증 포함 `curl http://localhost:3910/v1/models`
  3. 필요 시 위 SSH 터널을 다시 연다.

## DB/Redis 터널 정보
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "SSH Tunnel (DB/Redis)",
      "type": "shell",
      "command": "ssh -f -N -i \"/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/.ssh/samsung_ai_portal_dbs\" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60 -o ServerAliveCountMax=3 -o ConnectTimeout=10 -o ExitOnForwardFailure=yes -p 2194 -L 5432:localhost:5432 -L 6379:localhost:6379 samsung_ai_dbs@1.241.20.229 && sleep 2",
      "options": {
        "cwd": "/Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend"
      }
    }
  ]
}


## WAS 실행 스크립트
- /Users/bhkim/10_project/01_samsung_asset/samsung_ai_portal_backend/scripts/start.sh
