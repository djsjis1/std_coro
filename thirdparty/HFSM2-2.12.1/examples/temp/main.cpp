// HFSM2 (hierarchical state machine for games and interactive applications)
// Created by Andrew Gresyk

////////////////////////////////////////////////////////////////////////////////

#define HFSM2_ENABLE_PLANS
#include <hfsm2/machine.hpp>

#include <string>
#include <vector>
#include <cstdio>
#include <cstring>

struct Payload
{
	std::string label;              // stack-use-after-return
	std::vector<int> data;          // memory leak
};

struct Context {};

using Config = hfsm2::Config::ContextT<Context>
							::PayloadT<Payload>
							::TaskCapacityN<256>;

using M = hfsm2::MachineT<Config>;

#define S(s) struct s
using FSM = M::Root<S(Root),
				S(StateA),
				S(StateB)
			>;
#undef S

struct Root
	: FSM::State
{};

struct StateA
	: FSM::State
{
	void enter(PlanControl& control)
	{
		Payload p;
		p.label = "";
		p.data  = {1, 2, 3, 4, 5};
		control.plan().changeWith<StateA, StateB>(p);
	}

	void update(FullControl& control) {
		control.succeed();
	}
};

struct StateB : FSM::State
{
	void enter(PlanControl& control)
	{
		const auto& transitions = control.currentTransitions();
		const Payload* p = transitions[0].payload();
		std::string copy = p->label;
		std::printf("[StateB] data.size() = %zu, data[0] = %d\n",
					p->data.size(),
					p->data.empty() ? -1 : p->data[0]);
	}
};

int main()
{
	Context ctx;
	FSM::Instance fsm{ctx};

	fsm.update();
	fsm.update();

	return 0;
}

////////////////////////////////////////////////////////////////////////////////
