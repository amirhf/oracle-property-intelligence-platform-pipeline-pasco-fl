import { beforeAll, describe, expect, it } from "vitest";

import {
  FIXTURE_DEFINITIONS,
  FrozenContractValidator,
  type FixtureName,
} from "../../src/contracts/validate.js";

let validator: FrozenContractValidator;

beforeAll(async () => {
  validator = await FrozenContractValidator.create();
});

describe("frozen contract fixtures", () => {
  for (const fixture of Object.keys(FIXTURE_DEFINITIONS) as FixtureName[]) {
    it(`validates ${fixture} against its intended definition`, async () => {
      expect(
        validator.validateFixture(
          fixture,
          await validator.loadFixture(fixture),
        ),
      ).toBeUndefined();
    });
  }

  it("matches every hash in the frozen contract lock, including the MCP schema", async () => {
    expect(await validator.verifyLockedHashes()).toEqual([]);
  });
});

describe("negative fixture mutations", () => {
  const mutations: Array<{
    fixture: FixtureName;
    mutate: (fixture: any) => void;
    name: string;
  }> = [
    {
      fixture: "error-response.json",
      name: "rejects an unsupported error code",
      mutate: (fixture) => {
        fixture.result.error.code = "made_up";
      },
    },
    {
      fixture: "permit-response.json",
      name: "rejects a malformed permit identifier",
      mutate: (fixture) => {
        fixture.result.data.permitId = "not-a-permit-id";
      },
    },
    {
      fixture: "property-response.json",
      name: "rejects a county outside the frozen contract",
      mutate: (fixture) => {
        fixture.result.data.county = "lee";
      },
    },
    {
      fixture: "search-request.json",
      name: "rejects a radius above the 50-mile bound",
      mutate: (fixture) => {
        fixture.arguments.radius.value = 50.0001;
      },
    },
    {
      fixture: "search-response.json",
      name: "rejects an unrecognized match reason",
      mutate: (fixture) => {
        fixture.result.data.opportunities[0].matchReasons = ["model_guess"];
      },
    },
  ];

  for (const mutation of mutations) {
    it(mutation.name, async () => {
      const fixture = await validator.loadFixture(mutation.fixture);
      mutation.mutate(fixture);
      expect(
        validator.validateFixture(mutation.fixture, fixture),
      ).toBeDefined();
    });
  }
});
