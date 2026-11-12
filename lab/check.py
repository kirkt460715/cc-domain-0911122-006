import subprocess,sys,time
start=time.monotonic()
steps=[['python3','-m','unittest','discover','-s','tests','-v'],['python3','-m','compileall','-q','trialvault','lab','tests'],['python3', '-m', 'trialvault', 'demo'],['python3','-m','trialvault.harvest_demo'],['python3','-m','lab.demo']]
for command in steps:
    print('运行:', ' '.join(command),flush=True)
    result=subprocess.run(command,timeout=75)
    if result.returncode:sys.exit(result.returncode)
print('全部完成，用时',round(time.monotonic()-start,3),'秒',flush=True)
