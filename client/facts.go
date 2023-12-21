package client

type CompanyFacts struct {
	CIK        uint32                            `json:"cik"`
	EntityName string                            `json:"entityName"`
	Facts      map[string]map[string]CompanyFact `json:"facts"`
}

type CompanyFact struct {
	Label       string                `json:"label"`
	Description string                `json:"description"`
	Units       map[string][]FactUnit `json:"units"`
}

type FactUnit struct {
	Start string  `json:"start"`
	End   string  `json:"end"`
	Val   float64 `json:"val"`
	Accn  string  `json:"accn"`
	FY    uint    `json:"fy"`
	FP    string  `json:"fp"`
	Form  string  `json:"form"`
	Filed string  `json:"filed"`
	Frame string  `json:"frame"`
}
