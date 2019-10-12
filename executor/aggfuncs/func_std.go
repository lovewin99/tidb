package aggfuncs

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"math"
)

type baseStdAggFunc struct {
	baseAggFunc
}

type std4Float64 struct {
	baseStdAggFunc
	isSamp      bool
	hasDistinct bool
}

type partialResult4StdFloat64 struct {
	valSlice []float64
	sum      float64
	isNull   bool
}

func (e *std4Float64) AllocPartialResult() PartialResult {
	p := new(partialResult4StdFloat64)
	p.isNull = true
	p.valSlice = make([]float64, 0)
	return PartialResult(p)
}

func (e *std4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StdFloat64)(pr)
	p.valSlice = make([]float64, 0)
	p.sum = 0
	p.isNull = true
}

func (e *std4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StdFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if e.hasDistinct {
			exist := false
			for _, v := range p.valSlice {
				if v == input {
					exist = true
					break
				}
			}
			if exist {
				continue
			}
		}
		if p.isNull {
			p.sum = input
			p.valSlice = append(p.valSlice, input)
			p.isNull = false
			continue
		}
		p.valSlice = append(p.valSlice, input)
		p.sum += input
	}
	return nil
}

func (e *std4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4StdFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	//1, 找出平均数
	avg := p.sum / float64(len(p.valSlice))
	//2, 找出方差
	var res float64
	for _, o := range p.valSlice {
		res += (o - avg) * (o - avg)
	}
	var v float64
	if e.isSamp {
		size := float64(len(p.valSlice) - 1)
		if size <= 0 {
			chk.AppendNull(e.ordinal)
			return nil
		} else {
			v = res / size
		}
	} else {
		v = res / float64(len(p.valSlice))
	}
	v = math.Sqrt(v)
	chk.AppendFloat64(e.ordinal, v)
	return nil
}

func (e *std4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4StdFloat64)(src), (*partialResult4StdFloat64)(dst)
	if p1.isNull {
		return nil
	}

	p2.sum = p1.sum + p2.sum
	p2.isNull = false
	for _, v := range p1.valSlice {
		p2.valSlice = append(p2.valSlice, v)
	}
	return nil
}

type std4Decimal struct {
	baseStdAggFunc
	isSamp      bool
	hasDistinct bool
}
type partialResult4StdDecimal struct {
	valSlice []types.MyDecimal
	sum      types.MyDecimal
	isNull   bool
}

func (*std4Decimal) AllocPartialResult() PartialResult {
	p := new(partialResult4StdDecimal)
	p.isNull = true
	p.valSlice = make([]types.MyDecimal, 0)
	return PartialResult(p)
}

func (*std4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4StdDecimal)(pr)
	p.valSlice = make([]types.MyDecimal, 0)
	p.isNull = true
}

func (e *std4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4StdDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			continue
		}
		if e.hasDistinct {
			exist := false
			for _, v := range p.valSlice {
				if v == *input {
					exist = true
					break
				}
			}
			if exist {
				continue
			}
		}
		if p.isNull {
			p.valSlice = append(p.valSlice, *input)
			p.sum = *input
			p.isNull = false
			continue
		}

		sum := p.sum
		err = types.DecimalAdd(&sum, input, &p.sum)
		if err != nil {
			return errors.Trace(err)
		}
		p.valSlice = append(p.valSlice, *input)
	}

	return nil
}

func (e *std4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	var err error
	p := (*partialResult4StdDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	//1, 找出平均数
	var avg types.MyDecimal
	count := types.NewDecFromInt(int64(len(p.valSlice)))
	err = types.DecimalDiv(&p.sum, count, &avg, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	//2, 找出方差
	var res = new(types.MyDecimal)
	for _, o := range p.valSlice {
		//sub = 单个值 - 平均值
		sub := new(types.MyDecimal)
		err = types.DecimalSub(&o, &avg, sub)
		if err != nil {
			return errors.Trace(err)
		}
		// sub * sub 求平方
		mul := new(types.MyDecimal)
		sub2 := *sub
		err = types.DecimalMul(sub, &sub2, mul)
		if err != nil {
			return errors.Trace(err)
		}
		//将求完平方的值加起来
		res2 := *res
		types.DecimalAdd(&res2, mul, res)
	}
	//3, (单值 - avg) ^ 2 / 单值的个数
	x := new(types.MyDecimal)
	if e.isSamp {
		size := int64(len(p.valSlice) - 1)
		if size <= 0 {
			chk.AppendNull(e.ordinal)
			return nil
		} else {
			x = types.NewDecFromInt(size)
		}
	} else {
		x = types.NewDecFromInt(int64(len(p.valSlice)))
	}
	v := new(types.MyDecimal)
	err = types.DecimalDiv(res, x, v, types.DivFracIncr)
	if err != nil {
		return errors.Trace(err)
	}
	//开方
	d2f, err := v.ToFloat64()
	v = types.NewDecFromFloatForTest(math.Sqrt(d2f))
	chk.AppendMyDecimal(e.ordinal, v)
	return nil
}

func (e *std4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4StdDecimal)(src), (*partialResult4StdDecimal)(dst)
	if p1.isNull {
		return nil
	}

	newSum := new(types.MyDecimal)
	err := types.DecimalAdd(&p1.sum, &p2.sum, newSum)
	if err != nil {
		return errors.Trace(err)
	}
	p2.sum = *newSum

	for _, k := range p1.valSlice {
		p2.valSlice = append(p2.valSlice, k)
	}
	p2.isNull = false

	return nil
}
