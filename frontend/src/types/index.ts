export interface Machine {
  id: number;
  machine_type: string | null;
  brand: string | null;
  model: string | null;
  price: number | null;
  currency: string;
  location: string | null;
  description: string | null;
  machine_url: string;
  website_source: string | null;
  thumbnail_url: string | null;
  thumbnail_local: string | null;
  created_at: string;
  updated_at: string | null;
  images: MachineImage[];
  specs: MachineSpec[];
}

export interface MachineImage {
  id: number;
  image_url: string;
  local_path: string | null;
  is_primary: boolean;
}

export interface MachineSpec {
  spec_key: string;
  spec_value: string | null;
  spec_unit: string | null;
}

export interface SearchResponse {
  query: string;
  total: number;
  page: number;
  limit: number;
  pages: number;
  results: SearchResultItem[];
}

export interface SearchResultItem {
  id: number;
  machine_type: string | null;
  brand: string | null;
  model: string | null;
  price: number | null;
  currency: string;
  location: string | null;
  thumbnail_url: string | null;
  machine_url: string;
  website_source: string | null;
  created_at: string;
}

export interface SearchFilters {
  query: string;
  machine_type?: string;
  brand?: string;
  location?: string;
  price_min?: number;
  price_max?: number;
  sort_by?: string;
  page?: number;
  limit?: number;
}

export interface Website {
  id: number;
  name: string;
  url: string;
  description: string | null;
  is_active: boolean;
  crawl_enabled: boolean;
  machine_count: number;
  last_crawled_at: string | null;
  crawl_status: string;
  created_at: string;
}

export interface User {
  id: number;
  email: string;
  full_name: string | null;
  is_active: boolean;
  is_admin: boolean;
  created_at: string;
}

export interface AuthToken {
  access_token: string;
  token_type: string;
  user: User;
}

export interface TrainingRules {
  id: number;
  website_id: number;
  listing_selector:     string | null;
  title_selector:       string | null;
  url_selector:         string | null;
  description_selector: string | null;
  image_selector:       string | null;
  price_selector:       string | null;
  category_selector:    string | null;
  pagination_selector:  string | null;
  created_at: string;
  updated_at: string | null;
}

export type TrainingRulesForm = Omit<TrainingRules, "id" | "website_id" | "created_at" | "updated_at">;
