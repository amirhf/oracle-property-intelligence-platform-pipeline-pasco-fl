export interface SourceParseCount {
  accepted: number;
  parsed: number;
  rejectionReasons: Record<string, number>;
  rejected: number;
  source: number;
}

export type SourceParseCounts = Record<string, SourceParseCount>;

export interface ParcelSourceRow {
  acres: number | null;
  exactFolio: string;
  heatedSquareFeet: number | null;
  homestead: string | null;
  neighborhoodCode: string | null;
  propertyUseCode: string | null;
  propertyUseDescription: string | null;
  totalSquareFeet: number | null;
}

export interface BuildingSourceRow {
  actualYearBuilt: number | null;
  buildingNumber: string;
  buildingSection: string;
  effectiveYearBuilt: number | null;
  exactFolio: string;
  heatedSquareFeet: number | null;
  observedCondition: string | null;
  roofCover: string | null;
  roofStructure: string | null;
  stories: number | null;
  totalSquareFeet: number | null;
  useDescription: string | null;
}

export interface SiteAddressSourceRow {
  city: string | null;
  exactFolio: string;
  siteAddress: string;
  zipCode: string | null;
}

export interface OwnerSourceRow {
  exactFolio: string;
  mailingAddress1: string | null;
  mailingAddress2: string | null;
  mailingCity: string | null;
  mailingCountry: string | null;
  mailingState: string | null;
  mailingZip: string | null;
  ownerName1: string | null;
  ownerName2: string | null;
}

export interface PilotCandidate {
  buildings: BuildingSourceRow[];
  parcel: ParcelSourceRow;
  siteAddress: SiteAddressSourceRow | null;
}

export interface PilotSelectionEntry extends PilotCandidate {
  propertyId: string;
  rank: string;
  useGroup: string;
  yearBucket: string;
  yearBuilt: number;
}

export interface CoordinateResult {
  latitude: number;
  longitude: number;
  method: "polygon_centroid";
  sourceCrs: "EPSG:4326";
  sourceLastUpdate: string | null;
}

export interface AccelaPermitRow {
  address: string | null;
  description: string | null;
  projectName: string | null;
  recordDate: string | null;
  recordNumber: string;
  recordType: string;
  status: string | null;
}

export interface ArtifactCapture {
  bytes: number;
  localPath: string;
  readyMarkerPath: string;
  sha256: string;
  sourceSystem: string;
  sourceUrl: string;
}

export interface PreparedProperty extends PilotSelectionEntry {
  coordinates: CoordinateResult | null;
  owners: OwnerSourceRow[];
  permits: AccelaPermitRow[];
}

export interface PreparedPilot {
  artifacts: ArtifactCapture[];
  gisMetrics: GisAcquisitionMetrics;
  permitRequestCount: number;
  properties: PreparedProperty[];
  resourceMetrics: {
    diskAvailableBytes: number;
    elapsedMs: number;
    peakRssBytes: number;
  };
  sampleAlgorithm: string;
  sampleSeed: string;
  selectedRecordSha256: string;
  selectionSize: number;
  snapshotId: string;
  snapshotManifestSha256: string;
  sourceCounts: SourceParseCounts;
  sourceLimitations: string[];
}

export interface GisAcquisitionMetrics {
  batchCount: number;
  batchSize: number;
  concurrency: number;
  requestCount: number;
  retryCount: number;
  reusedBatchCount: number;
  statusCounts: Record<string, number>;
}

export interface PilotRunRequest {
  asOf: string;
  county: "pasco";
  expectedSnapshotId?: string;
  runId: string;
  sampleAlgorithm: string;
  sampleSeed: string;
  selectionSize: 25 | 5_000 | 25_000;
  workflowId: string;
}

export interface PilotRunSummary {
  acceptedProperties: number;
  activeProperties: number;
  buildings: number;
  // Content-hash delta counts remain independent of lifecycle transitions.
  changedProperties: number;
  coordinates: number;
  databaseGrowthBytes: number;
  databaseSizeAfterBytes: number;
  databaseSizeBeforeBytes: number;
  diskAvailableBytes: number;
  duplicateProperties: number;
  elapsedMs: number;
  explicitUnavailableFacts: number;
  gisMetrics: GisAcquisitionMetrics;
  inactiveProperties: number;
  inactivatedProperties: number;
  missingCoordinates: number;
  newProperties: number;
  ownership: number;
  peakRssBytes: number;
  permitRequestCount: number;
  permits: number;
  rejectedRecords: number;
  reactivatedProperties: number;
  roofSignals: number;
  roofSignalBasis: Record<string, number>;
  runId: string;
  selectionSize: number;
  sourceCounts: SourceParseCounts;
  throughputPropertiesPerSecond: number;
  unchangedProperties: number;
  workflowId: string;
}
