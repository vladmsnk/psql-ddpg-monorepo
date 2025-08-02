package autovacuum

import (
	"strconv"
)

type SettingDto struct {
	name    string `db:"name"`
	setting string `db:"setting"`
}

func ToVacuumStats(dtos []SettingDto) (VacuumStats, error) {
	s := VacuumStats{}

	for _, d := range dtos {

		switch d.name {
		case "autovacuum_vacuum_cost_limit":
			costLimit, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumVacuumCostLimit = int32(costLimit)
		case "autovacuum_vacuum_cost_delay":
			val, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumVacuumCostDelay = int32(val)
		case "autovacuum_work_mem":
			val, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumWorkMem = int32(val)
		case "autovacuum_max_workers":
			val, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumMaxWorkers = int32(val)
		case "autovacuum_vacuum_threshold":
			val, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumVacuumThreshold = int32(val)
		case "autovacuum_vacuum_scale_factor":
			val, _ := strconv.ParseFloat(d.setting, 64)
			s.AutovacuumVacuumScaleFactor = val
		case "autovacuum_analyze_threshold":
			val, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumAnalyzeThreshold = int32(val)
		case "autovacuum_analyze_scale_factor":
			val, _ := strconv.ParseFloat(d.setting, 64)
			s.AutovacuumAnalyzeScaleFactor = val
		case "autovacuum_freeze_max_age":
			val, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumFreezeMaxAge = int32(val)
		case "autovacuum_multixact_freeze_max_age":
			val, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumMultixactFreezeMaxAge = int32(val)
		case "autovacuum_naptime":
			val, _ := strconv.ParseInt(d.setting, 10, 32)
			s.AutovacuumNaptimeSeconds = int32(val)
		}
	}
	return s, nil
}
