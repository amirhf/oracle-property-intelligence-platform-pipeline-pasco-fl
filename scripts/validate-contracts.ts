import { FrozenContractValidator } from "../src/contracts/validate.js";

const validator = await FrozenContractValidator.create();
const result = await validator.validateAll();
const ok =
  result.fixtureFailures.length === 0 && result.hashFailures.length === 0;

console.log(JSON.stringify({ ok, ...result }, null, 2));
if (!ok) process.exitCode = 1;
