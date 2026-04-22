# SSH 접속 정보

> 민감 정보가 포함된 운영 문서다. 외부 공유를 금지한다.

## LLM 터널 정보
- 로컬 : ssh -L 3910:localhost:3950 minisoft@1.241.20.229 -p 2194
- was : ssh -L 3900:localhost:3950 minisoft@1.241.20.229 -p 2194
- 암호 (로컬/was 동일) : Mini1234!

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
