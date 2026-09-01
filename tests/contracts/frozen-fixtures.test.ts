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
      fixture: "service-info-request.json",
      name: "rejects service-info input fields",
      mutate: (fixture) => {
        fixture.arguments.county = "pasco";
      },
    },
    {
      fixture: "service-info-response.json",
      name: "rejects an incomplete supported-tool list",
      mutate: (fixture) => {
        fixture.result.data.supportedTools.pop();
      },
    },
    {
      fixture: "pipeline-run-summary-request.json",
      name: "rejects pipeline-summary input fields",
      mutate: (fixture) => {
        fixture.arguments.latest = true;
      },
    },
    {
      fixture: "pipeline-run-summary-response.json",
      name: "rejects zero counts for unavailable permit coverage",
      mutate: (fixture) => {
        fixture.result.data.coverage.permits.recordCount = 0;
      },
    },
    {
      fixture: "property-request.json",
      name: "rejects arbitrary property lookup paths",
      mutate: (fixture) => {
        fixture.arguments.path = "../../properties.parquet";
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
      fixture: "permit-request.json",
      name: "rejects arbitrary permit lookup SQL",
      mutate: (fixture) => {
        fixture.arguments.sql = "select * from permits";
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
      fixture: "property-response.json",
      name: "rejects an empty available current-owner list",
      mutate: (fixture) => {
        fixture.result.data.ownership.currentOwners.value = [];
      },
    },
    {
      fixture: "property-response.json",
      name: "rejects an available owner name without value provenance",
      mutate: (fixture) => {
        delete fixture.result.data.ownership.currentOwners.value[0]
          .evidenceRefs;
      },
    },
    {
      fixture: "property-response.json",
      name: "rejects inferred ownership classification",
      mutate: (fixture) => {
        fixture.result.data.ownership.classification = {
          availability: "available",
          value: "individual",
          class: "inferred",
          evidenceRefs: ["ev_fixture_appraiser_001"],
        };
      },
    },
    {
      fixture: "property-response.json",
      name: "rejects empty mailing address lines as available",
      mutate: (fixture) => {
        fixture.result.data.ownership.publicMailingAddress.value.addressLines.value =
          [];
      },
    },
    {
      fixture: "property-response.json",
      name: "rejects an invented value in an unavailable phone fact",
      mutate: (fixture) => {
        fixture.result.data.ownership.phone.value = "+1-555-0100";
      },
    },
    {
      fixture: "property-response.json",
      name: "rejects a malformed available email fact",
      mutate: (fixture) => {
        fixture.result.data.ownership.email = {
          availability: "available",
          value: "not-an-email",
          class: "raw",
          evidenceRefs: ["ev_fixture_appraiser_001"],
        };
      },
    },
    {
      fixture: "property-response.json",
      name: "rejects extra ownership fields",
      mutate: (fixture) => {
        fixture.result.data.ownership.acquisitionDate = "2020-01-01";
      },
    },
    {
      fixture: "property-response.json",
      name: "rejects an unapproved publication status",
      mutate: (fixture) => {
        fixture.result.data.ownership.privacy.publicationStatus = "private";
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
    {
      fixture: "query-schema-request.json",
      name: "rejects query-schema input fields",
      mutate: (fixture) => {
        fixture.arguments.includeInternal = true;
      },
    },
    {
      fixture: "query-schema-response.json",
      name: "rejects an unrestricted SQL capability",
      mutate: (fixture) => {
        fixture.result.data.queryRestrictions.arbitrarySql = true;
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

describe("record and coverage distinctions", () => {
  it("accepts zero search matches as a successful response", async () => {
    const fixture: any = await validator.loadFixture("search-response.json");
    fixture.result.data.opportunities = [];
    expect(
      validator.validateFixture("search-response.json", fixture),
    ).toBeUndefined();
  });

  it("accepts not_found for a missing individual record", async () => {
    const fixture: any = await validator.loadFixture("error-response.json");
    fixture.tool = "prism_v1_get_property";
    fixture.result.error.code = "not_found";
    fixture.result.error.message = "The requested property does not exist.";
    expect(
      validator.validateFixture("error-response.json", fixture),
    ).toBeUndefined();
  });

  it("accepts null permit aggregates when permit coverage is unavailable", async () => {
    const fixture: any = await validator.loadFixture("search-response.json");
    const property = fixture.result.data.opportunities[0].property;
    property.openRoofingPermitCount = {
      availability: "unavailable",
      value: null,
      class: "derived",
      reason: "source_unavailable",
      evidenceRefs: [],
    };
    property.maximumOpenRoofingPermitDays = {
      availability: "unavailable",
      value: null,
      class: "derived",
      reason: "source_unavailable",
      evidenceRefs: [],
    };
    property.permits = [];
    expect(
      validator.validateFixture("search-response.json", fixture),
    ).toBeUndefined();
  });
});

describe("ownership publication semantics", () => {
  it("requires every available owner/contact evidence reference to resolve", async () => {
    const fixture: any = await validator.loadFixture("property-response.json");
    fixture.result.data.ownership.currentOwners.value[0].evidenceRefs = [
      "ev_missing_owner_source",
    ];
    expect(
      validator.validateFixture("property-response.json", fixture),
    ).toBeDefined();
  });

  it("rejects substituting the situs address for public mailing", async () => {
    const fixture: any = await validator.loadFixture("property-response.json");
    fixture.result.data.address.value =
      "900 EXAMPLE RECORD AVENUE, SAMPLEVILLE, FL 00000, US";
    expect(
      validator.validateFixture("property-response.json", fixture),
    ).toBeDefined();
  });

  it("accepts fictional available ownership with unavailable phone and email", async () => {
    const fixture: any = await validator.loadFixture("property-response.json");
    const ownership = fixture.result.data.ownership;
    expect(ownership.currentOwners.value).toHaveLength(2);
    expect(ownership.phone).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    expect(ownership.email).toMatchObject({
      availability: "unavailable",
      value: null,
    });
    expect(
      validator.validateFixture("property-response.json", fixture),
    ).toBeUndefined();
  });
});
