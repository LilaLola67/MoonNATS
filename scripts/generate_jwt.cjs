// Synthetic operator/account/user credentials for local integration tests only.
// Claim layout follows https://github.com/nats-io/jwt (Apache-2.0).
const fs = require('node:fs');
const crypto = require('node:crypto');
function b32(bytes) {
  const alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';let value=0,bits=0,out='';
  for(const byte of bytes){value=(value<<8)|byte;bits+=8;while(bits>=5){bits-=5;out+=alphabet[(value>>>bits)&31];value&=(1<<bits)-1;}}
  if(bits)out+=alphabet[(value<<(5-bits))&31];return out;
}
function key(byte,prefix){
  const seed=Buffer.alloc(32,byte);
  const privateKey=crypto.createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b657004220420','hex'),seed]),format:'der',type:'pkcs8'});
  const pub=crypto.createPublicKey(privateKey).export({format:'der',type:'spki'}).subarray(-32);
  const data=Buffer.concat([Buffer.from([prefix]),pub]);let crc=0;
  for(const b of data){crc^=b<<8;for(let i=0;i<8;i++)crc=(crc&32768)?((crc<<1)^4129)&65535:(crc<<1)&65535;}
  return {privateKey,publicKey:b32(Buffer.concat([data,Buffer.from([crc&255,crc>>>8])]))};
}
function token(issuer,subject,type,nats){
  const claim={iat:Math.floor(Date.now()/1000),iss:issuer.publicKey,name:'MoonNATS synthetic '+type,sub:subject.publicKey,nats:{...nats,type,version:2}};
  claim.jti=b32(crypto.createHash('sha256').update(JSON.stringify(claim)).digest());
  const header=Buffer.from(JSON.stringify({typ:'JWT',alg:'ed25519-nkey'})).toString('base64url');
  const payload=Buffer.from(JSON.stringify(claim)).toString('base64url');
  const data=header+'.'+payload;
  return data+'.'+crypto.sign(null,Buffer.from(data),issuer.privateKey).toString('base64url');
}
const operator=key(1,112),account=key(2,0),user=key(0,160);
fs.mkdirSync('.test-data',{recursive:true});
fs.writeFileSync('.test-data/operator.jwt',token(operator,operator,'operator',{}));
const accountToken=token(operator,account,'account',{limits:{subs:-1,data:-1,payload:-1,imports:-1,exports:-1,wildcards:true,conn:-1,leaf:-1}});
fs.writeFileSync('.test-data/user.jwt',token(account,user,'user',{pub:{},sub:{},subs:-1,data:-1,payload:-1}));
fs.writeFileSync('.tools/jwt.local.conf','listen: "127.0.0.1:14227"\noperator: ".test-data/operator.jwt"\nresolver: MEMORY\nresolver_preload: { '+account.publicKey+': "'+accountToken+'" }\n');

// Public all-zero NKey vector, never a deployment credential.
fs.writeFileSync('.test-data/user.creds',
  '-----BEGIN NATS USER JWT-----\n'+fs.readFileSync('.test-data/user.jwt','utf8')+'\n------END NATS USER JWT------\n\n'+
  '-----BEGIN USER NKEY SEED-----\nSUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEQ\n------END USER NKEY SEED------\n');
